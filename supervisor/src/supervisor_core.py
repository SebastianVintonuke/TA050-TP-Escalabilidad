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

            # check importante para el nuevo lider
            if prev_state != STATE_DOWN:
                self.inactivity[node_id] += 1
                last_activity_register = self.inactivity[node_id]
            else:
                self.inactivity[node_id] += 1
                last_activity_register = self.inactivity[node_id]
                
                # re check por cada hard threshold
                if last_activity_register % self.hard_threshold == 0:
                    actions.append(("send_check", node_id))
                continue

            if prev_state == STATE_SUSPECT:
                #logging.debug(f"{node_id}: SUSPECT check - inactivity={last_activity_register}, hard={self.hard_threshold}, will_mark_down={last_activity_register >= self.hard_threshold}")
                pass
            
            if prev_state == STATE_SUSPECT and last_activity_register >= self.hard_threshold:
                #logging.warning(f"{node_id} SUSPECT -> DOWN (inactividad={last_activity_register}, hard_threshold={self.hard_threshold})")
                self.state[node_id] = STATE_DOWN
                actions.append(("mark_down", node_id))
                continue 

            # cruza el soft threshold desde UP → SUSPECT
            if last_activity_register == self.soft_threshold and prev_state == STATE_UP:
                self.state[node_id] = STATE_SUSPECT
                self.check_sent[node_id] = False
                actions.append(("send_check", node_id))

            elif prev_state == STATE_SUSPECT and last_activity_register > self.soft_threshold:
                # reenviar cada 2 ticks
                if (last_activity_register - self.soft_threshold) % 2 == 0:
                    actions.append(("send_check", node_id))

        return actions

    def get_state(self, node_id: str) -> str:
        return self.state[node_id]

    def get_inactivity(self, node_id: str) -> int:
        return self.inactivity[node_id]

    def get_last_timestamp(self, node_id: str) -> float:
        return self.last_timestamp[node_id]

    def get_all_states(self) -> Dict[str, str]:
        return dict(self.state)
    
    def get_state_snapshot(self) -> Dict:
        # estado actual del sistema
        snapshot = {}
        for node_id in self.inactivity.keys():
            snapshot[node_id] = {
                'state': self.state[node_id],
                'inactivity': self.inactivity[node_id],
                'last_timestamp': self.last_timestamp[node_id]
            }
        return snapshot
    
    def restore_state_from_replica(self, state_snapshot: Dict) -> List[Action]:
        actions: List[Action] = []
        
        for node_id in list(self.inactivity.keys()):
            if node_id not in state_snapshot:
                # no estaba en el snapshot del líder anterior, iniciar desde DOWN
                # logging.warning(f"restore_state: {node_id} not in previous state, starting as DOWN")
                self.state[node_id] = STATE_DOWN
                self.inactivity[node_id] = 0
                actions.append(("send_check", node_id))
                continue
            
            prev_state = state_snapshot[node_id]['state']
            prev_inactivity = state_snapshot[node_id]['inactivity']
            prev_timestamp = state_snapshot[node_id].get('last_timestamp', 0.0)
            
            # restaurar
            self.state[node_id] = prev_state
            self.inactivity[node_id] = prev_inactivity
            self.last_timestamp[node_id] = prev_timestamp
            
            # logging.info(f"restore_state: {node_id} restored as {prev_state} with inactivity={prev_inactivity}")
            
            # reinicio
            if prev_state == STATE_DOWN:
                if prev_inactivity >= self.soft_threshold:
                    # logging.info(f"restore_state: {node_id} was DOWN with high inactivity, marking for restart")
                    actions.append(("mark_down", node_id))
                actions.append(("send_check", node_id))
            
            # monitorear
            elif prev_state == STATE_SUSPECT:
                self.check_sent[node_id] = False
                actions.append(("send_check", node_id))
                # logging.info(f"restore_state: {node_id} was SUSPECT, sending check")
            
            # validar
            else:
                # marcar como SUSPECT == acelerar validacion
                self.state[node_id] = STATE_SUSPECT
                self.inactivity[node_id] = self.soft_threshold
                self.check_sent[node_id] = False
                actions.append(("send_check", node_id))
                # logging.info(f"restore_state: {node_id} was UP, marking as SUSPECT for validation")
        
        return actions
    
    def reset_for_new_leader(self) -> List[Action]:
        actions: List[Action] = []
        
        for node_id in list(self.inactivity.keys()):
            old_state = self.state[node_id]
            old_inactivity = self.inactivity[node_id]
            
            # probablemente caido
            if old_inactivity >= self.hard_threshold:
                if old_state != STATE_DOWN:
                    self.state[node_id] = STATE_DOWN
                    actions.append(("mark_down", node_id))
                    # logging.warning(f"reset_for_new_leader: {node_id} had high inactivity={old_inactivity}, marking DOWN")
                self.inactivity[node_id] = 0
                continue
            
            if old_state == STATE_DOWN:
                if old_inactivity >= self.soft_threshold:
                    self.inactivity[node_id] = 0
                    actions.append(("mark_down", node_id))
                    actions.append(("send_check", node_id))
                    # logging.info(f"reset_for_new_leader: {node_id} DOWN with high inactivity={old_inactivity}, marking for restart")
                else:
                    self.inactivity[node_id] = 0
                    actions.append(("send_check", node_id))
                    # logging.info(f"reset_for_new_leader: {node_id} DOWN (will re-check, inactivity=0)")
            elif old_state == STATE_SUSPECT:
                # re-check rápido
                self.inactivity[node_id] = self.soft_threshold
                self.check_sent[node_id] = False
                actions.append(("send_check", node_id))
                # logging.info(f"reset_for_new_leader: {node_id} SUSPECT (re-checking, inactivity={self.soft_threshold})")
            else:
                self.inactivity[node_id] = 0
                # logging.debug(f"reset_for_new_leader: {node_id} {old_state} (inactivity=0)")
        
        return actions
