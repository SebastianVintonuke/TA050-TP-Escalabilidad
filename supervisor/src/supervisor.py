import socket
import threading
import time
import logging
from typing import Dict, Tuple, List, Optional

from supervisor.src.supervisor_core import SupervisorCore
from supervisor.src.restart_manager import RestartManager
from supervisor.src.leader_election import BullyElection
from common.protocol.heartbeat_leader import (
    HeartbeatProtocol,
    LeaderElectionProtocol, 
    MSG_TYPE_NODE_ALIVE_RESPONSE,
    MSG_TYPE_WHO_IS_LEADER
)

ACTION_MARK_UP = "mark_up"
ACTION_SEND_CHECK = "send_check"
ACTION_MARK_DOWN = "mark_down"

LEADER_ID_INDEX = 0
LEADER_NAME_INDEX = 1
LEADER_PORT_INDEX = 2


class Supervisor:
    def __init__(
        self,
        nodes_config: Dict[str, Tuple[str, int]],
        supervisor_id: Optional[int],
        supervisor_peers: Optional[Dict[int, Tuple[str, int]]],
        enable_leader_election: bool,
        election_port: int,
        supervisor_port: int,
        heartbeat_tick: float,
        soft_threshold: int,
        hard_threshold: int,
        enable_restart: bool,
        max_restart_attempts: int,
        restart_window: float,
    ) -> None:
        self.nodes_config = nodes_config
        self.supervisor_port = supervisor_port
        self.heartbeat_tick = heartbeat_tick
        self.supervisor_id = supervisor_id
        self.supervisor_peers = supervisor_peers or {}
        self._stop_event = threading.Event()

        self._t_recv: Optional[threading.Thread] = None
        self._t_tick: Optional[threading.Thread] = None

        self._init_supervisor_core(soft_threshold, hard_threshold)

        self._init_sockets()

        self._init_restart_manager(enable_restart, max_restart_attempts, restart_window)

        self._init_leader_election(enable_leader_election, election_port)

    def _init_supervisor_core(self, soft_threshold: int, hard_threshold: int) -> None:
        self.core = SupervisorCore(
            nodes=list(self.nodes_config.keys()),
            soft_threshold=soft_threshold,
            hard_threshold=hard_threshold
        )

    def _init_sockets(self) -> None:
        self.recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.recv_sock.bind(("0.0.0.0", self.supervisor_port))
        self.recv_sock.settimeout(1.0)
        self.send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def _init_restart_manager(
        self, 
        enable_restart: bool, 
        max_restart_attempts: int, 
        restart_window: float
    ) -> None:
        self.enable_restart = enable_restart
        self.restart_manager: Optional[RestartManager] = None
        
        if self.enable_restart:
            try:
                self.restart_manager = RestartManager(
                    max_restart_attempts=max_restart_attempts,
                    restart_window=restart_window
                )
                logging.info("RestartManager initialized - restart enabled")
            except Exception as e:
                logging.warning(f"Failed to initialize RestartManager: {e}. Restart disabled.")
                self.enable_restart = False

    def _init_leader_election(self, enable_leader_election: bool, election_port: int) -> None:
        self.enable_leader_election = enable_leader_election
        self.bully: Optional[BullyElection] = None
        self._monitoring_active = True
        
        if self.enable_leader_election:
            if not self.supervisor_id or not self.supervisor_peers:
                raise ValueError("supervisor_id and supervisor_peers required for leader election")
            
            logging.info("Initializing Bully leader election")
            self._monitoring_active = False
            
            self.bully = BullyElection(
                supervisor_id=self.supervisor_id,
                supervisor_peers=self.supervisor_peers,
                election_port=election_port,
                election_timeout=3.0,
                alive_interval=2.0,
                alive_timeout=5.0,
            )
            self.bully.on_become_leader = self._on_become_leader
            self.bully.on_lose_leadership = self._on_lose_leadership
        else:
            logging.info("Running in standalone mode (no leader election)")

    def start(self) -> None:
        if self.bully:
            self.bully.start()
        
        self._t_recv = threading.Thread(target=self._recv_loop)
        self._t_recv.start()

        self._t_tick = threading.Thread(target=self._tick_loop)
        self._t_tick.start()

    def stop(self) -> None:
        self._stop_event.set()

        if self.bully:
            self.bully.stop()

        if self.restart_manager:
            self.restart_manager.stop()

        try:
            self.recv_sock.close()
        except OSError:
            pass

        try:
            self.send_sock.close()
        except OSError:
            pass

        if self._t_recv is not None:
            self._t_recv.join()

        if self._t_tick is not None:
            self._t_tick.join()


    def _recv_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                data, addr = self.recv_sock.recvfrom(2048)
            except socket.timeout:
                continue
            except OSError:
                break

            if len(data) == 0:
                continue

            msg_type = data[0]

            if msg_type == MSG_TYPE_WHO_IS_LEADER:
                self._handle_who_is_leader(data, addr)
                continue

            if msg_type == MSG_TYPE_NODE_ALIVE_RESPONSE:
                if not self._monitoring_active:
                    continue

                _, node_id, ts = HeartbeatProtocol.decode_message(data)
                
                if node_id not in self.nodes_config:
                    continue

                ack = HeartbeatProtocol.encode_heartbeat_ack(node_id)
                try:
                    self.send_sock.sendto(ack, addr)
                except OSError:
                    pass

                actions = self.core.register_heartbeat(node_id, ts)

                self._apply_actions(actions)

    def _tick_loop(self) -> None:
        while not self._stop_event.is_set():
            time.sleep(self.heartbeat_tick)

            if not self._monitoring_active:
                continue

            actions = self.core.tick()

            self._apply_actions(actions)

    def _apply_actions(self, actions: List[Tuple[str, str]]) -> None:
        for action, node_id in actions:
            if action == ACTION_MARK_UP:
                self.mark_node_active(node_id)

            elif action == ACTION_SEND_CHECK:
                self.check_node_health(node_id)

            elif action == ACTION_MARK_DOWN:
                self.handle_node_down(node_id)

    def handle_node_down(self, node_id):
        inact = self.core.get_inactivity(node_id)
        logging.error(f"{node_id} -> DOWN (inactividad={inact}, hard timeout)")
        if self.restart_manager:
            self.restart_manager.restart_node_with_backoff(node_id)
        else:
            logging.warning(f"{node_id} restart skipped (restart disabled)")

    def check_node_health(self, node_id):
        inact = self.core.get_inactivity(node_id)
        logging.warning(f"{node_id} -> SUSPECT (inactividad={inact}, enviando check)")
        self._send_node_alive_check(node_id)

    def mark_node_active(self, node_id):
        inact = self.core.get_inactivity(node_id)
        logging.info(f"{node_id} -> UP (inactividad reseteda desde {inact})")

        if self.restart_manager:
            self.restart_manager.clear_restart_history(node_id)

    def _send_node_alive_check(self, node_id: str) -> None:
        host, port = self.nodes_config[node_id]
        addr = (host, port)
        data = HeartbeatProtocol.encode_node_alive_check(node_id)

        try:
            self.send_sock.sendto(data, addr)
        except OSError:
            pass

    def _handle_who_is_leader(self, data: bytes, addr: Tuple[str, int]) -> None:
        node_id, _ = LeaderElectionProtocol.decode_who_is_leader(data)
        
        if not node_id:
            return
        
        if self._monitoring_active and self.supervisor_id:
            response = LeaderElectionProtocol.encode_leader_info(
                leader_id=self.supervisor_id,
                leader_host=socket.gethostname(),
                leader_port=self.supervisor_port
            )
            try:
                self.send_sock.sendto(response, addr)
                logging.debug(f"Responded to WHO_IS_LEADER from {node_id}: I am leader")
            except OSError:
                pass
        elif self.bully and self.bully.current_leader is not None:
            leader_info = None
            for sup_id, (sup_host, sup_election_port) in self.supervisor_peers.items():
                if sup_id == self.bully.current_leader:
                    leader_info = (sup_id, sup_host, self.supervisor_port)
                    break
            
            if leader_info:
                response = LeaderElectionProtocol.encode_leader_info(
                    leader_id=leader_info[LEADER_ID_INDEX],
                    leader_host=leader_info[LEADER_NAME_INDEX],
                    leader_port=leader_info[LEADER_PORT_INDEX]
                )
                try:
                    self.send_sock.sendto(response, addr)
                    logging.debug(f"Responded to WHO_IS_LEADER from {node_id}: leader is {leader_info[0]}")
                except OSError:
                    pass
            else:
                self._send_no_leader_yet(addr)
        else:
            self._send_no_leader_yet(addr)
    
    def _send_no_leader_yet(self, addr: Tuple[str, int]) -> None:
        response = LeaderElectionProtocol.encode_no_leader_yet()
        try:
            self.send_sock.sendto(response, addr)
        except OSError:
            pass
    
    def _on_become_leader(self) -> None:
        logging.info("BECAME LEADER - Activating node monitoring")
        self._monitoring_active = True
    
    def _on_lose_leadership(self) -> None:
        logging.warning("LOST LEADERSHIP - Deactivating node monitoring")
        self._monitoring_active = False
    
    def is_leader(self) -> bool:
        if not self.bully:
            return True
        return self.bully.is_leader()
    
    def get_leader_id(self) -> Optional[int]:
        if not self.bully:
            return self.supervisor_id
        return self.bully.get_leader_id()
