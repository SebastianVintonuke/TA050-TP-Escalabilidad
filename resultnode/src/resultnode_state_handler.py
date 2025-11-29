import json 
import logging
from common.state_storage.packet_id_tracker import PacketIDTracker

class UserQueryCounter:
	def __init__(self, user_query_id, handler):
		self.user_query_id = user_query_id
		self.handler = handler
		self.packet_tracker =PacketIDTracker()
		self.last_packet_id = -1

		self.version_id = 0		
		self.batch_ver_count = 1 # for now always 1?

	def is_eof(self):
		return self.last_packet_id >=0 and self.packet_tracker.handled_all_up_to(self.last_packet_id)

META_USER_QUERY_ID = "user_query_id" 
META_LAST_PACKET_ID = "last_packet_id" 
META_NEXT_EXP_PACKET = "next_exp_packet" 
META_MISSING = "missing_packets" 
META_MSGS_BATCH = "msg_count_batch"

## Base to serialize in changes and state
def serial_state(user_query_state):

	res = {
	}

	res[META_LAST_PACKET_ID] = user_query_state.last_packet_id
	res[META_NEXT_EXP_PACKET] = user_query_state.packet_tracker.expected_next_packet
	res[META_MISSING] = list(user_query_state.packet_tracker.missing_packets)

	return res

class ResultNodeStateManager:
	def __init__(self, get_user_query_state):
		self.get_user_query_state = get_user_query_state

	# does it inplace .. no issues with that.
	def apply_changes(self, user_query_state, changes):
		
		user_query_state.last_packet_id= changes[META_LAST_PACKET_ID]
		user_query_state.packet_tracker.expected_next_packet = changes[META_NEXT_EXP_PACKET]
		user_query_state.packet_tracker.missing_packets = changes[META_MISSING]

		return user_query_state


	def deserialize_state(self, state):

		state = json.loads(state.decode())
		user_query_state = self.get_user_query_state(state[META_USER_QUERY_ID])


		user_query_state.last_packet_id= changes[META_LAST_PACKET_ID]
		user_query_state.packet_tracker.expected_next_packet = changes[META_NEXT_EXP_PACKET]
		user_query_state.packet_tracker.missing_packets = changes[META_MISSING]

		user_query_state.deduce_pkt_counter()

		return user_query_state

	## Almost the same as state... i.e is basically like a snapshot... no user id needed, and adds msgs batch count...
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
