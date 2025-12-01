import time
from typing import Dict, List, Tuple, Iterable, Optional

STATE_UP = "UP"
STATE_SUSPECT = "SUSPECT"
STATE_DOWN = "DOWN"

Action = Tuple[str, str]

class SupervisorCore:
    def __init__(
        self,
        nodes: Iterable[str],
        soft_threshold: int,
        hard_threshold: int,
    ) -> None:
        if soft_threshold <= 0 or hard_threshold <= 0:
            raise ValueError("soft_threshold y hard_threshold deben ser > 0")
        if soft_threshold >= hard_threshold:
            raise ValueError("soft_threshold debe ser < hard_threshold")

        self.soft_threshold = soft_threshold
        self.hard_threshold = hard_threshold

        self.inactivity: Dict[str, int] = {}
        self.state: Dict[str, str] = {}
        self.last_timestamp: Dict[str, float] = {}
        self.check_sent: Dict[str, bool] = {}

        for node in nodes:
            self.inactivity[node] = 0
            # arranca en DOWN hasta recibir el primer heartbeat
            self.state[node] = STATE_DOWN
            self.last_timestamp[node] = 0.0

            self.check_sent[node] = False

    def register_heartbeat(self, node_id: str, timestamp: Optional[float] = None) -> List[Action]:
        if node_id not in self.inactivity:
            return []

        if timestamp is None:
            timestamp = time.time()

        actions: List[Action] = []
        prev_state = self.state[node_id]

        if timestamp <= self.last_timestamp[node_id]:
            return []

        if prev_state != STATE_DOWN:
            self.inactivity[node_id] = 0
            self.last_timestamp[node_id] = timestamp

            if prev_state != STATE_UP:
                self.state[node_id] = STATE_UP
                actions.append(("mark_up", node_id))
        else:
            self.inactivity[node_id] = 0
            self.last_timestamp[node_id] = timestamp
            self.state[node_id] = STATE_UP
            actions.append(("mark_up", node_id))

        return actions

    def tick(self) -> List[Action]:
        actions: List[Action] = []

        for node_id in list(self.inactivity.keys()):
            prev_state = self.state[node_id]

            if prev_state == STATE_DOWN:
                continue

            self.inactivity[node_id] += 1
            last_activity_register = self.inactivity[node_id]

            # cruza el soft threshold → SUSPECT
            if last_activity_register == self.soft_threshold and prev_state == STATE_UP:
                self.state[node_id] = STATE_SUSPECT
                self.check_sent[node_id] = False
                actions.append(("send_check", node_id))

            # cruza el hard threshold → DOWN
            elif last_activity_register >= self.hard_threshold and prev_state == STATE_SUSPECT:
                self.state[node_id] = STATE_DOWN
                actions.append(("mark_down", node_id))

            # reenviar check si no hubo respuesta
            elif last_activity_register > self.soft_threshold and prev_state == STATE_SUSPECT and not self.check_sent[node_id]:
                actions.append(("send_check", node_id))
                self.check_sent[node_id] = True

        return actions

    def get_state(self, node_id: str) -> str:
        return self.state[node_id]

    def get_inactivity(self, node_id: str) -> int:
        return self.inactivity[node_id]

    def get_last_timestamp(self, node_id: str) -> float:
        return self.last_timestamp[node_id]

    def get_all_states(self) -> Dict[str, str]:
        return dict(self.state)
