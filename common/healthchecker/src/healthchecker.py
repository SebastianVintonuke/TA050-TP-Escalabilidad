import logging
import socket
import threading
import time
from typing import Optional, List, Dict, Any

from common.protocol.heartbeat_leader import (
    HeartbeatProtocol,
    LeaderElectionProtocol, 
    MSG_TYPE_NODE_ALIVE_CHECK,
    MSG_TYPE_LEADER_INFO,
    MSG_TYPE_NO_LEADER_YET,
    MSG_TYPE_HEARTBEAT_ACK
)


class Healthchecker:
    def __init__(
        self,
        node_id: str,
        supervisors: List[Dict[str, Any]],
        node_health_port: int,
        heartbeat_interval: float,
        heartbeat_timeout: float,
        discovery_timeout: float,
        discovery_retries: int,
    ) -> None:
        self.node_id = node_id
        self.supervisors = supervisors
        self.node_health_port = node_health_port
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.discovery_timeout = discovery_timeout
        self.discovery_retries = discovery_retries

        self.current_leader: Optional[Dict[str, Any]] = None

        self.send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.send_sock.settimeout(self.heartbeat_timeout)

        self.recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.recv_sock.bind(("0.0.0.0", self.node_health_port))
        self.recv_sock.settimeout(1.0)

        self._stop_event = threading.Event()
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._check_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=False)
        self._heartbeat_thread.start()

        self._check_thread = threading.Thread(target=self._check_listener_loop, daemon=False)
        self._check_thread.start()

    def run(self) -> None:
        try:
            self.start()
            
            self._stop_event.wait()
        finally:
            self.stop()

    def stop(self) -> None:
        if not self._stop_event.is_set():
            self._stop_event.set()
        
        self.recv_sock.close()
        self.send_sock.close()
        
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join()
        if self._check_thread and self._check_thread.is_alive():
            self._check_thread.join()

    def _discover_leader(self) -> bool:
        for attempt in range(self.discovery_retries):
            logging.info(f"Leader discovery attempt {attempt + 1}/{self.discovery_retries}")
            
            responses = []

            for supervisor in self.supervisors:
                msg = LeaderElectionProtocol.encode_who_is_leader(self.node_id)
                try:
                    self.send_sock.sendto(msg, (supervisor["host"], supervisor["port"]))
                except OSError as e:
                    logging.warning(f"Failed to send WHO_IS_LEADER to {supervisor}: {e}")
                    continue

                original_timeout = self.send_sock.gettimeout()
                self.send_sock.settimeout(self.discovery_timeout)
                try:
                    data, addr = self.send_sock.recvfrom(2048)
                    msg_type = data[0] if len(data) > 0 else None

                    if msg_type == MSG_TYPE_LEADER_INFO:
                        leader_id, leader_host, leader_port = LeaderElectionProtocol.decode_leader_info(data)
                        responses.append({
                            "id": leader_id,
                            "host": leader_host,
                            "port": leader_port
                        })
                        logging.info(f"Received LEADER_INFO: leader_id={leader_id}")
                    elif msg_type == MSG_TYPE_NO_LEADER_YET:
                        logging.debug(f"Supervisor {supervisor} has no leader yet")
                except socket.timeout:
                    logging.debug(f"No response from {supervisor}")
                except OSError:
                    break
                finally:
                    self.send_sock.settimeout(original_timeout)

            # elegir líder con mayor ID
            if responses:
                leader = max(responses, key=lambda x: x["id"])
                self.current_leader = leader
                logging.info(f"Leader discovered: {leader}")
                return True

            time.sleep(0.5)

        logging.error("Failed to discover leader after all retries")
        return False

    def _heartbeat_loop(self) -> None:
        while not self._stop_event.is_set():
            if self.current_leader is None:
                logging.info("No current leader, discovering...")
                self._discover_leader()
            else:
                if not self._send_heartbeat_to_leader():
                    logging.warning(f"Leader {self.current_leader} is not responding, rediscovering...")
                    self.current_leader = None
                    self._discover_leader()

            time.sleep(self.heartbeat_interval)

    def _send_heartbeat_to_leader(self, validate_node_id: bool = False) -> bool:
        if not self.current_leader:
            return False

        msg = HeartbeatProtocol.encode_node_alive_response(self.node_id)
        addr = (self.current_leader["host"], self.current_leader["port"])
        
        # reintento en heartbeats
        max_attempts = 2
        for attempt in range(max_attempts):
            try:
                self.send_sock.sendto(msg, addr)
                data, _ = self.send_sock.recvfrom(2048)
                msg_type = data[0] if len(data) > 0 else None
                
                if msg_type == MSG_TYPE_HEARTBEAT_ACK:
                    if validate_node_id:
                        node_id = HeartbeatProtocol.decode_heartbeat_ack(data)
                        return node_id == self.node_id
                    return True
            except (socket.timeout, OSError) as e:
                if attempt < max_attempts - 1:
                    logging.debug(f"Heartbeat attempt {attempt + 1} failed, retrying...")
                    time.sleep(0.05)
                else:
                    logging.debug(f"Heartbeat to leader failed after {max_attempts} attempts: {e}")
        
        return False

    def _check_listener_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                data, addr = self.recv_sock.recvfrom(2048)
            except socket.timeout:
                continue
            except OSError:
                break

            msg_type = data[0] if len(data) > 0 else None
            
            if msg_type == MSG_TYPE_NODE_ALIVE_CHECK:
                _, node_id, _ = HeartbeatProtocol.decode_message(data)
                if node_id == self.node_id:
                    if self.current_leader:
                        response = HeartbeatProtocol.encode_node_alive_response(self.node_id)
                        leader_addr = (self.current_leader["host"], self.current_leader["port"])
                        try:
                            self.send_sock.sendto(response, leader_addr)
                            logging.debug(f"Responded to NODE_ALIVE_CHECK from {addr}, sent to leader {leader_addr}")
                        except OSError as e:
                            logging.warning(f"Failed to send NODE_ALIVE_RESPONSE to leader: {e}")
