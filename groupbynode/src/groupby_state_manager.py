# from common.state_storage.json_state_manager import JSONStateManager
import json
"""

For groupby "state" is the previous QueryAccumulator , specifically the "groups" 
but also has the logic encapsulated.to avoid searching for that logic many times
For now use json
"""

FIELD_NEW_STATE = "groups"
META_ACUM_ID = "accumulator_id" 
META_EXP_MSG_COUNT = "exp_msg_count" 
META_MSG_COUNT = "msg_count" 

META_MSGS_TAGS = "msg_tags"
## State is :dict
class GroupbyStateManager:
	def __init__(self, get_accumulator):
		self.get_accumulator = get_accumulator

	# does it inplace .. no issues with that.
	# Changes is basically new group to be saved... 
	def apply_changes(self, query_accum, changes):
		query_accum.groups = changes[FIELD_NEW_STATE]
		query_accum.messages_received= changes[META_MSG_COUNT]
		query_accum.known_message_len= changes[META_EXP_MSG_COUNT]
		return query_accum


	def deserialize_state(self, state):
		state = json.loads(str(state))
		accum = self.get_accumulator(state[META_ACUM_ID])

		accum.groups = state[FIELD_NEW_STATE]
		accum.messages_received= state[META_MSG_COUNT]
		accum.known_message_len= state[META_EXP_MSG_COUNT]

		return accum


	## Almost the same as state... i.e is basically like a snapshot... but no acum id needed.
	def serialize_changes(self, query_accum):

		## Changes when serializing ... is ... in fact... also QueryAccumulator 
		print("------> GOT STATE! ", query_accum.groups)
		return json.dumps({
			FIELD_NEW_STATE: query_accum.groups,
			META_MSG_COUNT: query_accum.messages_received,
			META_EXP_MSG_COUNT: query_accum.known_message_len,
			META_MSGS_TAGS: query_accum.messages_tags
			}).encode()

	def deserialize_changes(self, changes_bytes):
		res = json.loads(str(changes_bytes))
		return res, res[META_MSGS_TAGS]



	# The same because of how groupbynode works...
	def serialize_state(self, query_accum): # Serialize Query accum to metadata and groups and so on
		return json.dumps({
				META_ACUM_ID: query_accum.accum_id,
				FIELD_NEW_STATE: query_accum.groups,
				META_MSG_COUNT: query_accum.messages_received,
				META_EXP_MSG_COUNT: query_accum.known_message_len
			}).encode()

	def serialize_initial_state(self, query_accum):
		return json.dumps({
				META_ACUM_ID: query_accum.accum_id,
				FIELD_NEW_STATE: query_accum.groups,
				META_MSG_COUNT: query_accum.messages_received,
				META_EXP_MSG_COUNT: query_accum.known_message_len
			}).encode()
