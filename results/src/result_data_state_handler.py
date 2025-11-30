import json
from common.state_storage.packet_id_tracker import PacketIDTracker

class ResultQueryTracker:
	def __init__(self, user_query_id):
		self.user_query_id = user_query_id
		self.packet_tracker =PacketIDTracker()
		self.version_id = 0		
		self.batch_ver_count = 1
		self.data = []

META_USER_QUERY_ID = "user_query_id"
META_NEXT_EXP_PACKET = "next_exp_packet" 
META_MISSING = "missing_packets" 
META_MSGS_BATCH = "msg_count_batch"
DATA = "data"

def serial_state(user_query_state):
	res = {}
	res[META_NEXT_EXP_PACKET] = user_query_state.packet_tracker.expected_next_packet
	res[META_MISSING] = list(user_query_state.packet_tracker.missing_packets)
	res[DATA] = user_query_state.data
	return res

class ResultStateManager:
	def __init__(self, get_user_query_state):
		self.get_user_query_state = get_user_query_state

	def apply_changes(self, user_query_state, changes):
		user_query_state.packet_tracker.expected_next_packet = changes[META_NEXT_EXP_PACKET]
		user_query_state.packet_tracker.missing_packets = changes[META_MISSING]
		user_query_state.data = changes[DATA]
		return user_query_state

	def deserialize_state(self, state):
		state = json.loads(state.decode())
		user_query_state = self.get_user_query_state(state[META_USER_QUERY_ID])
		user_query_state.packet_tracker.expected_next_packet = state[META_NEXT_EXP_PACKET]
		user_query_state.packet_tracker.missing_packets = state[META_MISSING]
		user_query_state.data = state[DATA]
		return user_query_state

	def serialize_changes(self, user_query_state):
		res = serial_state(user_query_state)
		res[META_MSGS_BATCH] = user_query_state.batch_ver_count
		return json.dumps(res).encode()

	def deserialize_changes(self, changes_bytes):
		res = json.loads(changes_bytes.decode())
		msg_count = res.pop(META_MSGS_BATCH)
		return res, msg_count

	def serialize_state(self, user_query_state):
		res = serial_state(user_query_state)
		res[META_USER_QUERY_ID]= user_query_state.user_query_id
		return json.dumps(res).encode()

	def serialize_initial_state(self, user_query_state):
		res = serial_state(user_query_state)
		res[META_USER_QUERY_ID]= user_query_state.user_query_id
		return json.dumps(res).encode()
