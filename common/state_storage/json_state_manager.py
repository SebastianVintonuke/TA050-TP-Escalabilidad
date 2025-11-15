from .base_state_manager import BaseStateManager
import json

class JSONStateManager(BaseStateManager):
	def apply_changes(self, state, changes):
		return state

	def serialize_state(self, state):
		return json.dumps(state).encode()
	def serialize_changes(self, changes):
		return json.dumps(changes).encode()

	def deserialize_state(self, state_bytes):
		if state_bytes == b"":
			return 
		return json.loads(str(state_bytes))

	def deserialize_changes(self, changes_bytes):
		return json.loads(str(changes_bytes))

	def empty_state(self):
		return b""