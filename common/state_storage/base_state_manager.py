class BaseStateManager:
	def apply_changes(self, state, changes):
		return state

	def serialize_state(self, state):
		return str(state).encode()
	def serialize_changes(self, changes):
		return "\n".join([str(change) in  changes]).encode()

	def deserialize_state(self, state_bytes):
		return state_bytes

	def deserialize_changes(self, changes_bytes):
		return changes_bytes

	def serialize_initial_state(self, metadata):
		return b""
