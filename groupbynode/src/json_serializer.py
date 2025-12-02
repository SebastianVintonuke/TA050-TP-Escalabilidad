# from common.state_storage.json_state_manager import JSONStateManager
import json
import logging
from common.state_storage.packet_id_tracker import PacketIDTracker

"""

For groupby "state" is the previous QueryAccumulator , specifically the "groups" 
but also has the logic encapsulated.to avoid searching for that logic many times
For now use json
"""


FIELD_GROUPS_KEY = "groups_keys"
FIELD_GROUPS_STATE = "groups_state"
META_ACUM_ID = "accumulator_id" 
META_LAST_PACKET_ID = "last_packet_id" 


META_NEXT_EXP_PACKET = "next_exp_packet" 
META_MISSING_PACKETS = "missing_packets" 

META_VERSION_COUNT_BATCH = "msg_count_batch"


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
		META_NEXT_EXP_PACKET: query_accum.packet_tracker.expected_next_packet,
		META_MISSING_PACKETS: list(query_accum.packet_tracker.missing_packets),
		META_LAST_PACKET_ID: query_accum.last_packet_id,
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

		query_accum.packet_tracker.expected_next_packet = changes[META_NEXT_EXP_PACKET]
		query_accum.packet_tracker.missing_packets = set(changes[META_MISSING_PACKETS])


		query_accum.last_packet_id= changes[META_LAST_PACKET_ID]
		return query_accum


	def deserialize_state(self, state):

		# print(f"STR STATE TO DESERIAL? \n'{state}'")
		state = json.loads(state.decode())
		query_accum = self.get_accumulator(state[META_ACUM_ID])

		# print("------> DESERIALIZED STATE?!! ", state)

		for key, value in zip(state[FIELD_GROUPS_KEY], state[FIELD_GROUPS_STATE]):
			# Convert key that is a list from serial to a tuple
			query_accum.groups[tuple(key)] = value

		query_accum.packet_tracker.expected_next_packet = state[META_NEXT_EXP_PACKET]
		query_accum.packet_tracker.missing_packets = set(state[META_MISSING_PACKETS])

		query_accum.last_packet_id= state[META_LAST_PACKET_ID]

		return query_accum


	## Almost the same as state... i.e is basically like a snapshot... but no acum id needed.
	def serialize_changes(self, query_accum):

		## Changes when serializing ... is ... in fact... also QueryAccumulator 
		res = serial_acc(query_accum)
		res[META_VERSION_COUNT_BATCH] = query_accum.batch_ver_count

		# print("------> GOT STATE TO SERIALIZE JSON! ", res)
		return json.dumps(res).encode()

	def deserialize_changes(self, changes_bytes):
		res = json.loads(changes_bytes.decode())
		msg_count = res.pop(META_VERSION_COUNT_BATCH)
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
