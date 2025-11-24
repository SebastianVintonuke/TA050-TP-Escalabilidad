import json 
import logging
from .join_accumulator import JoinAccumulator
"""
        self.left_rows = []
        self.right_rows = []

        self.left_packet_tracker =PacketIDTracker()
        self.last_left_packet_id = -1
        self.left_finished = False

        self.right_packet_tracker =PacketIDTracker()
        self.last_right_packet_id = -1
        self.right_finished = False
"""

META_JOIN_ACC_ID = "join_acc_id" 

ROWS_LEFT = "left_rows" 
ROWS_RIGHT = "right_rows" 
ROWS_OUT = "out_rows" 

META_LAST_PACKET_ID = "last_packet_id" 
META_MISSING_PCK = "missing_packets" 

PREFIX_LEFT= "left_"
PREFIX_RIGHT= "right_"


LEFT_LAST_PACKET_ID = PREFIX_LEFT+META_LAST_PACKET_ID
RIGHT_LAST_PACKET_ID = PREFIX_RIGHT+META_LAST_PACKET_ID


META_NEXT_EXP_PACKET = "next_exp_packet"


LEFT_MISSING = PREFIX_LEFT+META_MISSING_PCK
RIGHT_MISSING = PREFIX_RIGHT+META_MISSING_PCK

LEFT_EXP_PACKET = PREFIX_LEFT+META_NEXT_EXP_PACKET
RIGHT_EXP_PACKET = PREFIX_RIGHT+META_NEXT_EXP_PACKET


META_VER_COUNT = "ver_count_batch"


## Base to serialize in changes and state
def serial_state(join_acc):

	res = {
	}

	res[LEFT_LAST_PACKET_ID] = join_acc.left_last_packet_id
	res[LEFT_MISSING] = list(join_acc.left_packet_tracker.missing_packets)
	res[LEFT_EXP_PACKET] = join_acc.left_packet_tracker.expected_next_packet

	res[RIGHT_LAST_PACKET_ID] = join_acc.right_last_packet_id
	res[RIGHT_MISSING] = list(join_acc.right_packet_tracker.missing_packets)
	res[RIGHT_EXP_PACKET] = join_acc.right_packet_tracker.expected_next_packet

	res[ROWS_LEFT] = join_acc.left_rows
	res[ROWS_RIGHT] = join_acc.right_rows
	res[ROWS_OUT] = join_acc.get_out_rows()

	return res

class JoinNodeStateManager:
	def __init__(self, get_join_acc):
		self.get_join_acc = get_join_acc

	# does it inplace .. no issues with that.
	def apply_changes(self, join_acc, changes):
		
		join_acc.left_last_packet_id = changes[LEFT_LAST_PACKET_ID]
		join_acc.left_packet_tracker.missing_packets = set(changes[LEFT_MISSING])
		join_acc.left_packet_tracker.expected_next_packet = changes[LEFT_EXP_PACKET]

		join_acc.right_last_packet_id = changes[RIGHT_LAST_PACKET_ID]
		join_acc.right_packet_tracker.missing_packets = set(changes[RIGHT_MISSING])
		join_acc.right_packet_tracker.expected_next_packet = changes[RIGHT_EXP_PACKET]

		join_acc.left_rows = changes[ROWS_LEFT]
		join_acc.right_rows = changes[ROWS_RIGHT]
		join_acc.set_result_rows(changes[ROWS_OUT])

		return join_acc


	def deserialize_state(self, state):

		state = json.loads(state.decode())
		join_acc = self.get_join_acc(state[META_JOIN_ACC_ID])

		join_acc.left_last_packet_id = state[LEFT_LAST_PACKET_ID]
		join_acc.left_packet_tracker.missing_packets = set(state[LEFT_MISSING])
		join_acc.left_packet_tracker.expected_next_packet = state[LEFT_EXP_PACKET]

		join_acc.right_last_packet_id = state[RIGHT_LAST_PACKET_ID]
		join_acc.right_packet_tracker.missing_packets = set(state[RIGHT_MISSING])
		join_acc.right_packet_tracker.expected_next_packet = state[RIGHT_EXP_PACKET]

		join_acc.reload_status()

		join_acc.left_rows = state[ROWS_LEFT]
		join_acc.right_rows = state[ROWS_RIGHT]

		join_acc.set_result_rows(state[ROWS_OUT])

		return join_acc

	## Almost the same as state... i.e is basically like a snapshot... no joiner_id needed, and adds msgs batch count...
	def serialize_changes(self, join_acc):

		res = serial_state(join_acc)
		res[META_VER_COUNT] = join_acc.batch_ver_count

		return json.dumps(res).encode()

	def deserialize_changes(self, changes_bytes):
		res = json.loads(changes_bytes.decode())
		msg_count = res.pop(META_VER_COUNT)
		return res, msg_count

	def serialize_state(self, join_acc):
		res = serial_state(join_acc)
		res[META_JOIN_ACC_ID]= join_acc.joiner_id
		return json.dumps(res).encode()

	def serialize_initial_state(self, join_acc):
		res = serial_state(join_acc)
		res[META_JOIN_ACC_ID]= join_acc.joiner_id
		return json.dumps(res).encode()
