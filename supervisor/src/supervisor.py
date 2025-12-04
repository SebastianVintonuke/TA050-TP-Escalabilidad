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
    MSG_TYPE_WHO_IS_LEADER,
    MSG_TYPE_STATE_REQUEST,
    MSG_TYPE_STATE_REPLICATION
)

ACTION_MARK_UP = "mark_up"
ACTION_SEND_CHECK = "send_check"
ACTION_MARK_DOWN = "mark_down"

LEADER_ID_INDEX = 0
LEADER_NAME_INDEX = 1
LEADER_PORT_INDEX = 2

REPLICATION_INTERVAL = 3.0


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
        self._t_replication: Optional[threading.Thread] = None

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
        self.replicated_state: Optional[Dict] = None
        
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
        
    def start(self) -> None:
        if self.bully:
            self.bully.start()
        
        self._t_recv = threading.Thread(target=self._recv_loop)
        self._t_recv.start()

        self._t_tick = threading.Thread(target=self._tick_loop)
        self._t_tick.start()
        
        if self.enable_leader_election:
            self._t_replication = threading.Thread(target=self._state_replication_loop)
            self._t_replication.start()

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
        
        if self._t_replication is not None:
            self._t_replication.join()


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

            if msg_type == MSG_TYPE_STATE_REQUEST:
                self._handle_state_request(data, addr)
                continue

            if msg_type == MSG_TYPE_STATE_REPLICATION:
                self._handle_state_replication(data, addr)
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
                    # logging.debug(f"Sent HEARTBEAT_ACK to {node_id}")
                except OSError as e:
                    logging.error(f"Failed to send HEARTBEAT_ACK to {node_id}: {e}")

                actions = self.core.register_heartbeat(node_id, ts)

                self._apply_actions(actions)

    def _tick_loop(self) -> None:
        while not self._stop_event.is_set():
            time.sleep(self.heartbeat_tick)

            if not self._monitoring_active:
                continue

            actions = self.core.tick()

            self._apply_actions(actions)
    
    def _state_replication_loop(self) -> None:
        while not self._stop_event.is_set():
            time.sleep(REPLICATION_INTERVAL)
            
            # solo el lider
            if not self._monitoring_active:
                continue
            
            if not self.supervisor_id:
                continue
            
            state = self.core.get_state_snapshot()
            
            for peer_id, (peer_host, peer_election_port) in self.supervisor_peers.items():
                if peer_id == self.supervisor_id:
                    continue
                
                try:
                    message = LeaderElectionProtocol.encode_state_replication(
                        self.supervisor_id, 
                        state
                    )
                    self.send_sock.sendto(message, (peer_host, self.supervisor_port))
                    # logging.debug(f"Replicated state to supervisor {peer_id}: {len(state)} nodes")
                except Exception as e:
                    # logging.warning(f"Failed to replicate state to supervisor {peer_id}: {e}")
                    pass

    def _apply_actions(self, actions: List[Tuple[str, str]]) -> None:
        for action, node_id in actions:
            if action == ACTION_MARK_UP:
                self.mark_node_active(node_id)

            elif action == ACTION_SEND_CHECK:
                self.check_node_health(node_id)

            elif action == ACTION_MARK_DOWN:
                self.handle_node_down(node_id)

    def handle_node_down(self, node_id):
        logging.error(f"{node_id} -> DOWN")
        if self.restart_manager:
            recent_attempts = self.restart_manager.get_restart_attempts(node_id)
            if recent_attempts > 0:
                logging.warning(f"{node_id} restart skipped (already {recent_attempts} attempts in progress/recent)")
            else:
                self.restart_manager.restart_node_with_backoff(node_id)
        else:
            logging.warning(f"{node_id} restart skipped (restart disabled)")

    def check_node_health(self, node_id):
        logging.warning(f"{node_id} -> SUSPECT")
        self._send_node_alive_check(node_id)

    def mark_node_active(self, node_id):
        logging.info(f"{node_id} -> UP")

        if self.restart_manager:
            self.restart_manager.clear_restart_history(node_id)

    def _send_node_alive_check(self, node_id: str) -> None:
        host, port = self.nodes_config[node_id]
        addr = (host, port)
        data = HeartbeatProtocol.encode_node_alive_check(node_id)

        try:
            self.send_sock.sendto(data, addr)
            # logging.debug(f"Sent NODE_ALIVE_CHECK to {node_id} at {addr}")
        except OSError as e:
            logging.warning(f"Failed to send NODE_ALIVE_CHECK to {node_id}: {e}")

    def _handle_who_is_leader(self, data: bytes, addr: Tuple[str, int]) -> None:
        node_id, _ = LeaderElectionProtocol.decode_who_is_leader(data)
        
        if not node_id:
            return
        
        if self._monitoring_active and self.supervisor_id:
            socket_hostname = socket.gethostname()
            if self.supervisor_id in self.supervisor_peers:
                socket_hostname = self.supervisor_peers[self.supervisor_id][0]
            
            response = LeaderElectionProtocol.encode_leader_info(
                leader_id=self.supervisor_id,
                leader_host=socket_hostname,
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
                    # logging.debug(f"Responded to WHO_IS_LEADER from {node_id}: leader is {leader_info[0]}")
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
        
        # intento recuperar estado
        previous_state = self._request_state_from_previous_leader()
        
        if previous_state:
            logging.info("BECAME LEADER - Restoring state from previous leader")
            actions = self.core.restore_state_from_replica(previous_state)
        else:
            logging.info("BECAME LEADER - No previous state available, starting fresh")
            actions = self.core.reset_for_new_leader()
        
        logging.info(f"BECAME LEADER - applying {len(actions)} actions")
        self._apply_actions(actions)
    
    def _on_lose_leadership(self) -> None:
        logging.warning("LOST LEADERSHIP - Deactivating node monitoring")
        self._monitoring_active = False
    
    def _request_state_from_previous_leader(self) -> Optional[Dict]:
        if self.replicated_state:
            # logging.info(f"Using replicated state: {len(self.replicated_state)} nodes")
            return self.replicated_state
        
        if not self.bully or not self.bully.previous_leader:
            # logging.info("No previous leader to request state from")
            return None
        
        previous_leader_id = self.bully.previous_leader
        if previous_leader_id not in self.supervisor_peers:
            # logging.warning(f"Previous leader {previous_leader_id} not in peers")
            return None
        
        prev_host, prev_election_port = self.supervisor_peers[previous_leader_id]
        
        logging.info(f"Requesting state from previous leader {previous_leader_id} at {prev_host}:{self.supervisor_port}")
        
        request_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        request_sock.settimeout(2.0)
        
        try:
            if not self.supervisor_id:
                return None
            request = LeaderElectionProtocol.encode_state_request(self.supervisor_id)
            request_sock.sendto(request, (prev_host, self.supervisor_port))
            
            data, _ = request_sock.recvfrom(8192)
            state = LeaderElectionProtocol.decode_state_response(data)
            
            if state:
                logging.info(f"Received state from previous leader: {len(state)} nodes")
            return state
            
        except socket.timeout:
            logging.warning(f"Timeout waiting for state from previous leader {previous_leader_id}")
            return None
        except Exception as e:
            logging.error(f"Error requesting state from previous leader: {e}")
            return None
        finally:
            request_sock.close()
    
    def _handle_state_request(self, data: bytes, addr: Tuple[str, int]) -> None:
        try:
            requesting_supervisor_id = LeaderElectionProtocol.decode_state_request(data)
            # logging.info(f"Received state request from supervisor {requesting_supervisor_id}")
            
            state = self.core.get_state_snapshot()
            
            response = LeaderElectionProtocol.encode_state_response(state)
            self.send_sock.sendto(response, addr)
            
            # logging.info(f"Sent state snapshot to supervisor {requesting_supervisor_id}: {len(state)} nodes")
            
        except Exception as e:
            logging.error(f"Error handling state request: {e}")
    
    def _handle_state_replication(self, data: bytes, addr: Tuple[str, int]) -> None:
        try:
            result = LeaderElectionProtocol.decode_state_replication(data)
            if not result:
                logging.warning("Failed to decode state replication message")
                return
            
            leader_id, state = result
            
            # solo aceptar del lider
            if self.bully and self.bully.current_leader == leader_id:
                self.replicated_state = state
                # logging.debug(f"Stored replicated state from leader {leader_id}: {len(state)} nodes")
            else:
                current = self.bully.current_leader if self.bully else None
                # logging.warning(f"Ignoring state replication from {leader_id} (current leader: {current})")
                
        except Exception as e:
            logging.error(f"Error handling state replication: {e}")
    
    def is_leader(self) -> bool:
        if not self.bully:
            return True
        return self.bully.is_leader()
    
    def get_leader_id(self) -> Optional[int]:
        if not self.bully:
            return self.supervisor_id
        return self.bully.get_leader_id()
