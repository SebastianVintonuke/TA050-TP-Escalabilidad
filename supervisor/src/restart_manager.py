import docker
import logging
import threading
import time
from typing import Dict, List, Tuple, Optional

# reduce logs HTTP del docker client
logging.getLogger("urllib3").setLevel(logging.WARNING)

PAUSED_CONTAINER = 'paused'
EXITED_CONTAINER = 'exited'
DEAD_CONTAINER = 'dead'
CREATED_CONTAINER = 'created'

SEARCHED_CONTAINER = 0

class RestartManager:
    def __init__(
        self,
        max_restart_attempts: int,
        restart_window: float,
    ) -> None:
        self.max_restart_attempts = max_restart_attempts
        self.restart_window = restart_window

        self.docker_client: Optional[docker.DockerClient] = None
        try:
            self.docker_client = docker.from_env()
            logging.info("Docker client initialized")
        except Exception as e:
            logging.error(f"Failed to initialize Docker client: {e}")
            raise
        
        self.restart_history: Dict[str, List[Tuple[float, float]]] = {}

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._restart_threads: List[threading.Thread] = []

    def restart_node_with_backoff(self, node_id: str) -> None:
        if not self.docker_client:
            logging.error("Docker client not available")
            return

        now = time.time()

        with self._lock:
            if node_id not in self.restart_history:
                self.restart_history[node_id] = []

            # limpiar intentos fuera de la ventana
            self.restart_history[node_id] = [
                (timestamp, delay)
                for timestamp, delay in self.restart_history[node_id]
                if now - timestamp < self.restart_window
            ]

            attempt_count = len(self.restart_history[node_id])
            if attempt_count >= self.max_restart_attempts:
                logging.error(
                    f"{node_id} restart limit reached "
                    f"({attempt_count}/{self.max_restart_attempts} in last {self.restart_window}s). "
                    "Manual intervention required."
                )
                return

            # backoff: 2s, 4s, 8s, 16s, 30s
            backoff_delay = min(2 * (2 ** attempt_count), 30)
            self.restart_history[node_id].append((now, backoff_delay))

        logging.info(
            f"{node_id} scheduling restart (attempt {attempt_count + 1}/{self.max_restart_attempts}, "
            f"backoff={backoff_delay}s)"
        )

        restart_thread = threading.Thread(
            target=self._do_restart_after_delay,
            args=(node_id, backoff_delay),
            daemon=True,
        )

        with self._lock:
            self._restart_threads.append(restart_thread)

        restart_thread.start()

    def _do_restart_after_delay(self, node_id: str, delay: float) -> None:
        end_time = time.time() + delay
        while time.time() < end_time:
            if self._stop_event.is_set():
                logging.info(f"{node_id} restart cancelled due to shutdown")
                return
            time.sleep(0.5)

        if self._stop_event.is_set():
            return

        if not self.docker_client:
            return   
             
        try:
            # all=True incluye containers que no están 'running'
            containers = self.docker_client.containers.list(all=True, filters={"name": node_id})
            
            containers = [c for c in containers if c.name == node_id]
            
            if not containers:
                logging.error(f"{node_id} container not found for restart")
                return
            
            container = containers[SEARCHED_CONTAINER]
            status = container.status
            
            logging.info(f"{node_id} found container (status: {status})")
            
            # si el container está pausado, se debe despausar
            if status == PAUSED_CONTAINER:
                self.unpause_container(node_id, container)
            
            # si está stopped/exited, usar start() en vez de restart()
            if status in (EXITED_CONTAINER, DEAD_CONTAINER, CREATED_CONTAINER):
                self.start_container(node_id, container)
            else:
                self.restart_container(node_id, container)
            
            logging.info(f"{node_id} container restarted successfully")
            
        except docker.errors.NotFound:
            logging.error(f"{node_id} container not found for restart")
        except docker.errors.APIError as e:
            logging.error(f"{node_id} Docker API error during restart: {e}")
        except Exception as e:
            logging.error(f"{node_id} unexpected error during restart: {e}")

    def restart_container(self, node_id, container):
        logging.info(f"{node_id} restarting running container...")
        container.restart(timeout=10)

    def start_container(self, node_id, container):
        logging.info(f"{node_id} starting stopped container...")
        container.start()

    def unpause_container(self, node_id, container):
        logging.info(f"{node_id} container is paused, unpausing first...")
        container.unpause()
        time.sleep(1)
    
    def clear_restart_history(self, node_id: str) -> None:
        with self._lock:
            if node_id in self.restart_history:
                self.restart_history[node_id] = []
                # logging.debug(f"{node_id} restart history cleared")

    def get_restart_attempts(self, node_id: str) -> int:
        now = time.time()
        with self._lock:
            if node_id not in self.restart_history:
                return 0

            self.restart_history[node_id] = [
                (timestamp, delay)
                for timestamp, delay in self.restart_history[node_id]
                if now - timestamp < self.restart_window
            ]
            return len(self.restart_history[node_id])

    def stop(self) -> None:
        self._stop_event.set()

        with self._lock:
            threads = list(self._restart_threads)

        for thread in threads:
            thread.join()

        logging.info("RestartManager stopped cleanly")