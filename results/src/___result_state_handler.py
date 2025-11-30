
class ResultStateManager:
	def __init__(self):
		pass

	def apply_changes(self, user_query_file, changes):
		with user_query_file.open("a") as f:
			f.write(changes)

	def deserialize_state(self, state):
		pass

	def serialize_changes(self, new_lines):
		return new_lines.encode()

	def deserialize_changes(self, new_lines_bytes):
		return new_lines_bytes.decode()

	def serialize_state(self, user_query_state):
		pass

	def serialize_initial_state(self, user_query_state):
		pass
