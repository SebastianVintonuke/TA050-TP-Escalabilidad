# from common.state_storage.json_state_manager import JSONStateManager
import json
import logging







class QueryAccumulator:
	def __init__(self, accum_id, type_conf, msg_builder):
		self.type_conf = type_conf
		self.accum_id = accum_id
		self.msg_builder = msg_builder
		self.groups = {}
		self.batch_msg_count = 0
		self.messages_received=0
		self.known_message_len= -1
		self.rows_recv = 0
		self.msg_builder.reset_eof() # Ensure its not copying the eof flag from input sender

	def check(self, row):
		self.rows_recv+=1
		row = self.type_conf.map_input_row(row)
		key = self.type_conf.key_parser.get_group_key(row)

		acc = self.groups.get(key, None)
		if acc == None:
			acc = self.type_conf.grouper.new_group_acc(row)
			self.groups[key] = acc
		else:
			self.type_conf.grouper.add_group_acc(acc, row)
			#acc.add_row(row) # Better in design? who knows
		#print(f"{self.msg_builder.headers.types} HANDLED ", key, row,  acc)

	def send_built(self): # What happens If the groupbynode fails here/shutdowns here?
		for group, acc in self.groups.items():
			self.type_conf.add_output(self.msg_builder, group, acc)

		logging.info(f"Grouper node sending EOF rows processed {self.rows_recv} msg sent 1")
		self.type_conf.send(self.msg_builder)
		eof_signal = self.msg_builder.clone()
		eof_signal.set_as_eof(1)
		self.type_conf.send(eof_signal)

	def len_grouped(self):
		return len(self.groups)

	def describe(self):
		if len(self.groups) < 100:
			logging.info(f"curr status accumulator topk len {self.groups}:")
			for group, acc in self.groups.items():
				logging.info(f"key {group} acc : {acc}")

	def add_msg_count(self):
		self.messages_received+=1
		return self.known_message_len>=0 and self.messages_received >= self.known_message_len

	def check_eof(self, count_messages):
		self.known_message_len = count_messages
		return count_messages <= self.messages_received



"""

For groupby "state" is the previous QueryAccumulator , specifically the "groups" 
but also has the logic encapsulated.to avoid searching for that logic many times
For now use json
"""


FIELD_GROUPS_KEY = "groups_keys"
FIELD_GROUPS_STATE = "groups_state"
META_ACUM_ID = "accumulator_id" 
META_EXP_MSG_COUNT = "exp_msg_count" 
META_MSG_COUNT = "msg_count" 

META_MSGS_BATCH = "msg_count_batch"


## Base to serialize in changes and in state
def serial_acc(query_accum):
	keys = []
	vals = []
	for key, value in query_accum.groups.items():
		# Convert key that is a tuple to list to be able to put it in a json?
		keys.append(list(key))
		vals.append(value)

	return {
		FIELD_GROUPS_KEY: keys,
		FIELD_GROUPS_STATE: vals,
		META_MSG_COUNT: query_accum.messages_received,
		META_EXP_MSG_COUNT: query_accum.known_message_len,
	}

## State is :dict
class GroupbyStateManager:
	def __init__(self, get_accumulator):
		self.get_accumulator = get_accumulator

	# does it inplace .. no issues with that.
	# Changes is basically new group to be saved... 
	def apply_changes(self, query_accum, changes):
		for key, value in zip(changes[FIELD_GROUPS_KEY], changes[FIELD_GROUPS_STATE]):
			# Convert key that is a list from serial to a tuple
			query_accum.groups[tuple(key)] = value

		query_accum.messages_received= changes[META_MSG_COUNT]
		query_accum.known_message_len= changes[META_EXP_MSG_COUNT]
		return query_accum


	def deserialize_state(self, state):

		# print(f"STR STATE TO DESERIAL? \n'{state}'")
		state = json.loads(state.decode())
		query_accum = self.get_accumulator(state[META_ACUM_ID])

		# print("------> DESERIALIZED STATE?!! ", state)

		for key, value in zip(state[FIELD_GROUPS_KEY], state[FIELD_GROUPS_STATE]):
			# Convert key that is a list from serial to a tuple
			query_accum.groups[tuple(key)] = value

		query_accum.messages_received= state[META_MSG_COUNT]
		query_accum.known_message_len= state[META_EXP_MSG_COUNT]

		return query_accum


	## Almost the same as state... i.e is basically like a snapshot... but no acum id needed.
	def serialize_changes(self, query_accum):

		## Changes when serializing ... is ... in fact... also QueryAccumulator 
		res = serial_acc(query_accum)
		res[META_MSGS_BATCH] = query_accum.batch_msg_count

		# print("------> GOT STATE TO SERIALIZE JSON! ", res)
		return json.dumps(res).encode()

	def deserialize_changes(self, changes_bytes):
		res = json.loads(changes_bytes.decode())
		msg_count = res.pop(META_MSGS_BATCH)
		return res, msg_count



	# The same because of how groupbynode works...
	def serialize_state(self, query_accum): # Serialize Query accum to metadata and groups and so on
		res = serial_acc(query_accum)
		res[META_ACUM_ID]= query_accum.accum_id
		return json.dumps(res).encode()

	def serialize_initial_state(self, query_accum):
		res = serial_acc(query_accum)
		res[META_ACUM_ID]= query_accum.accum_id
		return json.dumps(res).encode()
