import json 
import logging

class UserCounter:
	def __init__(self, user_id):
		self.user_id = user_id
		self.pkt_id_counter = 0
		self.count_query_1 = 0
		self.count_query_2_profit = 0
		self.count_query_2_quantity = 0
		self.count_query_3 = 0
		self.count_query_4 = 0
		self.expected_count_query_1 = -1
		self.expected_count_query_2_profit = -1
		self.expected_count_query_2_quantity = -1
		self.expected_count_query_3 = -1
		self.expected_count_query_4 = -1

		self.batch_msg_count = 1 # for now always 1?

	## Essentially initial pkt id counter is the sum of recv packets, no need to save it separately.
	def deduce_pkt_counter(self):
		self.pkt_id_counter = (
			self.count_query_1 + self.count_query_3 + self.count_query_4
			+ self.count_query_2_profit + self.count_query_2_quantity
		)


	def is_eof_q1(self):
		return self.expected_count_query_1 >=0 and self.count_query_1 >= self.expected_count_query_1
	def is_eof_q2_profit(self):
		return self.expected_count_query_2_profit >=0 and self.count_query_2_profit >= self.expected_count_query_2_profit
	def is_eof_q2_quantity(self):
		return self.expected_count_query_2_quantity >=0 and self.count_query_2_quantity >= self.expected_count_query_2_quantity
	
	def is_eof_q3(self):
		return self.expected_count_query_3 >=0 and self.count_query_3 >= self.expected_count_query_3
	def is_eof_q4(self):
		return self.expected_count_query_4 >=0 and self.count_query_4 >= self.expected_count_query_4

META_USER_ID = "user_id" 
META_EXP_MSG_COUNT = "exp_msg_count" 
META_MSG_COUNT = "msg_count" 

PREFIX_Q1= "q1_"
PREFIX_Q2BS= "q2_bst_sell_"
PREFIX_Q2PROFIT= "q2_profit_"
PREFIX_Q3= "q3_"
PREFIX_Q4= "q4_"

META_MSGS_BATCH = "msg_count_batch"


## Base to serialize in changes and state
def serial_state(user_state):

	res = {
	}

	res[PREFIX_Q1+META_EXP_MSG_COUNT] = user_state.expected_count_query_1
	res[PREFIX_Q1+META_MSG_COUNT] = user_state.count_query_1

	res[PREFIX_Q3+META_EXP_MSG_COUNT] = user_state.expected_count_query_3
	res[PREFIX_Q3+META_MSG_COUNT] = user_state.count_query_3

	res[PREFIX_Q4+META_EXP_MSG_COUNT] = user_state.expected_count_query_4
	res[PREFIX_Q4+META_MSG_COUNT] = user_state.count_query_4

	res[PREFIX_Q2BS +META_EXP_MSG_COUNT] = user_state.expected_count_query_2_quantity
	res[PREFIX_Q2BS +META_MSG_COUNT] = user_state.count_query_2_quantity

	res[PREFIX_Q2PROFIT +META_EXP_MSG_COUNT] = user_state.expected_count_query_2_profit
	res[PREFIX_Q2PROFIT +META_MSG_COUNT] = user_state.count_query_2_profit

	return res

class ResultNodeStateManager:
	def __init__(self, get_user_state):
		self.get_user_state = get_user_state

	# does it inplace .. no issues with that.
	def apply_changes(self, user_state, changes):
		
		user_state.expected_count_query_1= changes[PREFIX_Q1+META_EXP_MSG_COUNT]
		user_state.count_query_1= changes[PREFIX_Q1+META_MSG_COUNT]

		user_state.expected_count_query_3= changes[PREFIX_Q3+META_EXP_MSG_COUNT]
		user_state.count_query_3= changes[PREFIX_Q3+META_MSG_COUNT]

		user_state.expected_count_query_4= changes[PREFIX_Q4+META_EXP_MSG_COUNT]
		user_state.count_query_4= changes[PREFIX_Q4+META_MSG_COUNT]

		user_state.expected_count_query_2_quantity= changes[PREFIX_Q2BS +META_EXP_MSG_COUNT]
		user_state.count_query_2_quantity= changes[PREFIX_Q2BS +META_MSG_COUNT]

		user_state.expected_count_query_2_profit= changes[PREFIX_Q2PROFIT +META_EXP_MSG_COUNT]
		user_state.count_query_2_profit= changes[PREFIX_Q2PROFIT +META_MSG_COUNT]

		return user_state


	def deserialize_state(self, state):

		state = json.loads(state.decode())
		user_state = self.get_user_state(state[META_USER_ID])

		user_state.expected_count_query_1= state[PREFIX_Q1+META_EXP_MSG_COUNT]
		user_state.count_query_1= state[PREFIX_Q1+META_MSG_COUNT]

		user_state.expected_count_query_3= state[PREFIX_Q3+META_EXP_MSG_COUNT]
		user_state.count_query_3= state[PREFIX_Q3+META_MSG_COUNT]

		user_state.expected_count_query_4= state[PREFIX_Q4+META_EXP_MSG_COUNT]
		user_state.count_query_4= state[PREFIX_Q4+META_MSG_COUNT]

		user_state.expected_count_query_2_quantity= state[PREFIX_Q2BS +META_EXP_MSG_COUNT]
		user_state.count_query_2_quantity= state[PREFIX_Q2BS +META_MSG_COUNT]

		user_state.expected_count_query_2_profit= state[PREFIX_Q2PROFIT +META_EXP_MSG_COUNT]
		user_state.count_query_2_profit= state[PREFIX_Q2PROFIT +META_MSG_COUNT]


		user_state.deduce_pkt_counter()

		return user_state

	## Almost the same as state... i.e is basically like a snapshot... no user id needed, and adds msgs batch count...
	def serialize_changes(self, user_state):

		res = serial_state(user_state)
		res[META_MSGS_BATCH] = user_state.batch_msg_count

		return json.dumps(res).encode()

	def deserialize_changes(self, changes_bytes):
		res = json.loads(changes_bytes.decode())
		msg_count = res.pop(META_MSGS_BATCH)
		return res, msg_count

	def serialize_state(self, user_state):
		res = serial_state(user_state)
		res[META_USER_ID]= user_state.user_id
		return json.dumps(res).encode()

	def serialize_initial_state(self, user_state):
		res = serial_state(user_state)
		res[META_USER_ID]= user_state.user_id
		return json.dumps(res).encode()
