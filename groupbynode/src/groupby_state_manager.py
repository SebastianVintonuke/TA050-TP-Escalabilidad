from common.state_storage.json_state_manager import JSONStateManager
import json
"""

For groupby "state" is the previous QueryAccumulator , specifically the "groups"
"""

## State is :dict
class GroupbyStateManager(JSONStateManager):
	def __init__(self, accumulators):
		self.accumulators = accumulators

	def apply_changes(self, state, changes):
		return state


	def serialize_state(self, state):
		return json.dumps(state.groups).encode()
	def serialize_changes(self, changes):
		return json.dumps(changes).encode()

	def deserialize_state(self, state_bytes):
		if state_bytes == b"":
			return 
		return json.loads(str(state_bytes))

	def deserialize_changes(self, changes_bytes):
		return json.loads(str(changes_bytes))

	def serialize_initial_state(self, metadata):
		return b""
