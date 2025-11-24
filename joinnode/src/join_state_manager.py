import json 
import logging
from .join_accumulator import JoinAccumulator
"""
        self.type_conf = type_conf
        self.msg_builder = msg_builder#type_conf.new_builder_for(msg, ind)
        self.msg_builder.reset_eof()# Ensure its not copying the eof flag from input sender

        self.left_rows = []
        self.right_rows = []
        self.left_finished = False
        self.right_finished = False
        self.limit = limit
        self.msg_sent = 0

        self.msg_expected_left = -1
        self.msg_expected_right = -1

        self.msg_count_left = 0
        self.msg_count_right = 0
"""

META_JOIN_ACC_ID = "join_acc_id" 

ROWS_LEFT = "left_rows" 
ROWS_RIGHT = "right_rows" 

META_EXP_MSG_COUNT = "exp_msg_count" 
META_MSG_COUNT = "msg_count" 

PREFIX_LEFT= "left_"
PREFIX_RIGHT= "right_"


LEFT_EXP_COUNT = PREFIX_LEFT+META_EXP_MSG_COUNT
RIGHT_EXP_COUNT = PREFIX_RIGHT+META_EXP_MSG_COUNT

LEFT_COUNT = PREFIX_LEFT+META_MSG_COUNT
RIGHT_COUNT = PREFIX_RIGHT+META_MSG_COUNT


META_MSGS_BATCH = "msg_count_batch"


## Base to serialize in changes and state
def serial_state(join_acc):

	res = {
	}

	res[LEFT_EXP_COUNT] = join_acc.msg_expected_left
	res[LEFT_COUNT] = join_acc.msg_count_left

	res[RIGHT_EXP_COUNT] = join_acc.msg_expected_right
	res[RIGHT_COUNT] = join_acc.msg_count_right

	res[ROWS_LEFT] = join_acc.left_rows
	res[ROWS_RIGHT] = join_acc.right_rows

	return res

class JoinNodeStateManager:
	def __init__(self, get_join_acc):
		self.get_join_acc = get_join_acc

	# does it inplace .. no issues with that.
	def apply_changes(self, join_acc, changes):
		
		join_acc.msg_expected_left = changes[LEFT_EXP_COUNT]
		join_acc.msg_count_left = changes[LEFT_COUNT]

		join_acc.msg_expected_right = changes[RIGHT_EXP_COUNT]
		join_acc.msg_count_right = changes[RIGHT_COUNT]

		join_acc.left_rows = changes[ROWS_LEFT]
		join_acc.right_rows = changes[ROWS_RIGHT]

		return join_acc


	def deserialize_state(self, state):

		state = json.loads(state.decode())
		join_acc = self.get_join_acc(state[META_JOIN_ACC_ID])

		join_acc.msg_expected_left = state[LEFT_EXP_COUNT]
		join_acc.msg_count_left = state[LEFT_COUNT]

		join_acc.msg_expected_right = state[RIGHT_EXP_COUNT]
		join_acc.msg_count_right = state[RIGHT_COUNT]

		join_acc.left_rows = state[ROWS_LEFT]
		join_acc.right_rows = state[ROWS_RIGHT]

		return join_acc

	## Almost the same as state... i.e is basically like a snapshot... no joiner_id needed, and adds msgs batch count...
	def serialize_changes(self, join_acc):

		res = serial_state(join_acc)
		res[META_MSGS_BATCH] = join_acc.batch_msg_count

		return json.dumps(res).encode()

	def deserialize_changes(self, changes_bytes):
		res = json.loads(changes_bytes.decode())
		msg_count = res.pop(META_MSGS_BATCH)
		return res, msg_count

	def serialize_state(self, join_acc):
		res = serial_state(join_acc)
		res[META_JOIN_ACC_ID]= join_acc.joiner_id
		return json.dumps(res).encode()

	def serialize_initial_state(self, join_acc):
		res = serial_state(join_acc)
		res[META_JOIN_ACC_ID]= join_acc.joiner_id
		return json.dumps(res).encode()
