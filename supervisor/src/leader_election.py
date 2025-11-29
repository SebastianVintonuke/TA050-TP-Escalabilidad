import socket
import threading
import time
import logging
from typing import Dict, Tuple, Optional, Callable
from enum import Enum


class SupervisorState(Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


MSG_ELECTION = "ELECTION"
MSG_ANSWER = "ANSWER"
MSG_COORDINATOR = "COORDINATOR"
MSG_ALIVE = "ALIVE"


class BullyElection:
    def __init__(
        self,
        supervisor_id: int,
        supervisor_peers: Dict[int, Tuple[str, int]],
        election_port: int,
        election_timeout: float,
        alive_interval: float,
        alive_timeout: float,
    ) -> None:
        self.supervisor_id = supervisor_id
        self.supervisor_peers = supervisor_peers
        self.election_timeout = election_timeout
        self.alive_interval = alive_interval
        self.alive_timeout = alive_timeout

        self.state = SupervisorState.FOLLOWER
        self.current_leader: Optional[int] = None
        self.last_leader_alive = 0.0

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", election_port))
        self.sock.settimeout(0.5)

        self._stop_event = threading.Event()

        self._election_lock = threading.Lock()
        self._election_in_progress = False

        self._answer_event = threading.Event()
        
        self._state_lock = threading.Lock()

        self.on_become_leader: Optional[Callable[[], None]] = None
        self.on_lose_leadership: Optional[Callable[[], None]] = None

        self._thread_listen: Optional[threading.Thread] = None
        self._thread_monitor: Optional[threading.Thread] = None
        self._thread_alive: Optional[threading.Thread] = None
        self._alive_stop_event = threading.Event()


    def start(self) -> None:
        logging.info(
            f"Starting Bully election (ID={self.supervisor_id}, "
            f"peers={list(self.supervisor_peers.keys())})"
        )

        self._thread_listen = threading.Thread(
            target=self._listen_loop,
            daemon=True,
        )
        self._thread_listen.start()

        self._thread_monitor = threading.Thread(
            target=self._monitor_leader_loop,
            daemon=True,
        )
        self._thread_monitor.start()

        time.sleep(0.5)

        if self.current_leader is None:
            self.trigger_election()

    def stop(self) -> None:
        self._stop_event.set()
        self._alive_stop_event.set()

        try:
            self.sock.close()
        except OSError:
            pass

        for thread in (self._thread_listen, self._thread_monitor, self._thread_alive):
            if thread is not None and thread.is_alive():
                thread.join(timeout=2.0)

        logging.info("BullyElection stopped cleanly")

    def is_leader(self) -> bool:
        return self.state == SupervisorState.LEADER

    def get_leader_id(self) -> Optional[int]:
        return self.current_leader

    def trigger_election(self) -> None:
        if self._stop_event.is_set():
            return

        election_thread = threading.Thread(
            target=self._start_election,
            daemon=True,
        )
        election_thread.start()

    def _start_election(self) -> None:
        with self._election_lock:
            if self._election_in_progress:
                logging.debug("Election already in progress, skipping")
                return

            self._election_in_progress = True
            self._answer_event.clear()

        logging.info(f"Starting election (current leader: {self.current_leader})")
        
        with self._state_lock:
            self.state = SupervisorState.CANDIDATE

        # Supervisores con ID mayor
        higher_peers = {
            peer_id: addr
            for peer_id, addr in self.supervisor_peers.items()
            if peer_id > self.supervisor_id
        }

        # Si no hay supervisores con ID mayor, me autoproclamo líder
        if not higher_peers:
            logging.info("I have the highest ID, becoming leader immediately")
            self._become_leader()
            with self._election_lock:
                self._election_in_progress = False
            return

        logging.info(f"Sending ELECTION to higher IDs: {list(higher_peers.keys())}")
        for peer_id, (host, port) in higher_peers.items():
            self._send_message((host, port), MSG_ELECTION, str(self.supervisor_id))

        election_start = time.time()
        while time.time() - election_start < self.election_timeout:
            if self._answer_event.is_set():
                logging.info("Received ANSWER from higher ID, waiting for new coordinator")
                with self._state_lock:
                    self.state = SupervisorState.FOLLOWER
                with self._election_lock:
                    self._election_in_progress = False
                return

            if self._stop_event.is_set():
                with self._election_lock:
                    self._election_in_progress = False
                return

            time.sleep(0.1)

        # Nadie con ID mayor respondió → me autoproclamo líder
        with self._state_lock:
            current_state = self.state
        
        if current_state == SupervisorState.CANDIDATE and not self._stop_event.is_set():
            logging.info("Election timeout: no ANSWER received, becoming leader")
            self._become_leader()

        with self._election_lock:
            self._election_in_progress = False

    def _become_leader(self) -> None:
        with self._state_lock:
            old_state = self.state
            
            if old_state == SupervisorState.LEADER:
                return
            
            self.state = SupervisorState.LEADER
            self.current_leader = self.supervisor_id
            should_stop_alive_thread = (self._thread_alive is not None and self._thread_alive.is_alive())

        logging.info("BECAME LEADER - Broadcasting COORDINATOR")

        # Enviar COORDINATOR a todos
        for peer_id, (host, port) in self.supervisor_peers.items():
            if peer_id != self.supervisor_id:
                self._send_message((host, port), MSG_COORDINATOR, str(self.supervisor_id))

        if old_state != SupervisorState.LEADER and self.on_become_leader:
            self.on_become_leader()

        # Detener thread ALIVE anterior si existe
        if should_stop_alive_thread and self._thread_alive is not None:
            self._alive_stop_event.set()
            self._thread_alive.join(timeout=1.0)
        
        # Crear nuevo thread ALIVE
        self._alive_stop_event.clear()
        
        def _alive_wrapper() -> None:
            self._leader_alive_loop()

        self._thread_alive = threading.Thread(
            target=_alive_wrapper,
            daemon=True,
        )
        self._thread_alive.start()

    def _leader_alive_loop(self) -> None:
        logging.info("Started ALIVE broadcast loop")

        while not self._stop_event.is_set() and not self._alive_stop_event.is_set():
            with self._state_lock:
                current_state = self.state
            
            if current_state != SupervisorState.LEADER:
                logging.info("No longer leader, stopping ALIVE broadcast")
                break

            for peer_id, (host, port) in self.supervisor_peers.items():
                if peer_id != self.supervisor_id:
                    self._send_message((host, port), MSG_ALIVE, str(self.supervisor_id))

            time.sleep(self.alive_interval)

    def _monitor_leader_loop(self) -> None:
        while not self._stop_event.is_set():
            time.sleep(1.0)

            with self._state_lock:
                current_state = self.state
            
            if current_state == SupervisorState.LEADER:
                continue

            if self.current_leader is not None:
                time_since_alive = time.time() - self.last_leader_alive

                # timeout del líder → nueva elección
                if time_since_alive > self.alive_timeout:
                    logging.warning(
                        f"Leader {self.current_leader} timeout "
                        f"({time_since_alive:.1f}s > {self.alive_timeout}s). "
                        f"Starting election."
                    )
                    self.current_leader = None
                    self.trigger_election()

    def _listen_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                data, addr = self.sock.recvfrom(1024)
                message = data.decode().strip()

                if not message:
                    continue

                # MSG_TYPE:sender_id
                parts = message.split(":", 1)
                if len(parts) != 2:
                    logging.warning(f"Invalid message format: {message}")
                    continue

                msg_type, sender_id_str = parts
                sender_id = int(sender_id_str)

                self._handle_message(msg_type, sender_id, addr)

            except socket.timeout:
                continue
            except ValueError as e:
                logging.warning(f"Failed to parse message: {e}")
            except Exception as e:
                if not self._stop_event.is_set():
                    logging.error(f"Error in listen loop: {e}")

    def _handle_message(self, msg_type: str, sender_id: int, addr: Tuple[str, int]) -> None:
        if msg_type == MSG_ELECTION:
            self._handle_election(sender_id, addr)

        elif msg_type == MSG_ANSWER:
            self._handle_answer(sender_id)

        elif msg_type == MSG_COORDINATOR:
            self._handle_coordinator(sender_id)

        elif msg_type == MSG_ALIVE:
            self._handle_alive(sender_id)

        else:
            logging.warning(f"Unknown message type: {msg_type}")

    def _handle_election(self, sender_id: int, addr: Tuple[str, int]) -> None:
        logging.debug(f"Received ELECTION from {sender_id}")

        if sender_id < self.supervisor_id:
            # Tengo mayor prioridad → ANSWER
            logging.info(f"Received ELECTION from {sender_id} (lower ID), sending ANSWER")
            self._send_message(addr, MSG_ANSWER, str(self.supervisor_id))

            # Iniciar elección si soy seguidor
            with self._state_lock:
                current_state = self.state
            
            if current_state == SupervisorState.FOLLOWER:
                logging.info("Starting my own election after receiving ELECTION")
                self.trigger_election()

    def _handle_answer(self, sender_id: int) -> None:
        logging.info(f"Received ANSWER from {sender_id} (higher ID)")

        with self._state_lock:
            if self.state == SupervisorState.CANDIDATE:
                self._answer_event.set()
                self.state = SupervisorState.FOLLOWER

    def _handle_coordinator(self, sender_id: int) -> None:
        if self.supervisor_id > sender_id:
            logging.warning(
                f"Ignoring COORDINATOR from {sender_id} because "
                f"I have higher ID ({self.supervisor_id})"
            )
            return

        logging.info(f"Received COORDINATOR from {sender_id} - Accepting as leader")

        with self._state_lock:
            was_leader = (self.state == SupervisorState.LEADER)

            self.current_leader = sender_id
            self.last_leader_alive = time.time()

            if sender_id != self.supervisor_id:
                if was_leader:
                    self._alive_stop_event.set()
                self.state = SupervisorState.FOLLOWER

        if was_leader and self.on_lose_leadership:
            logging.warning("Lost leadership")
            self.on_lose_leadership()

    def _handle_alive(self, sender_id: int) -> None:
        if sender_id == self.current_leader:
            self.last_leader_alive = time.time()
            # logging.debug(f"Received ALIVE from leader {sender_id}")

    def _send_message(self, addr: Tuple[str, int], msg_type: str, data: str) -> None:
        try:
            message = f"{msg_type}:{data}".encode()
            self.sock.sendto(message, addr)

            if msg_type != MSG_ALIVE:
                logging.debug(f"Sent {msg_type} to {addr}")
                
        except Exception as e:
            if not self._stop_event.is_set():
                logging.error(f"Failed to send {msg_type} to {addr}: {e}")
