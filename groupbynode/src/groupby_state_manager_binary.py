# from common.state_storage.json_state_manager import JSONStateManager
from .query_accumulator import *

DELIMETER_FIELD = b","
DELIMETER_ROW = b"\n"

NEW_GRP_ACC = "new_grp_acc"

# DESERIAL_MAP = {
# 	'int': lambda byte_data: int(byte_data.decode()),
# 	'float': lambda byte_data: float(byte_data.decode()),
# 	'str': lambda byte_data: byte_data.decode(),
# }

## From str for now
DESERIAL_MAP = {
	'int': lambda str_data: int(str_data),
	'float': lambda str_data: float(str_data),
	'str': lambda str_data: str_data,
}

SERIAL_MAP = {
	'int': lambda vl: str(vl).encode(),
	'float': lambda vl: str(vl).encode(),
	'str': lambda vl: vl.encode(),
}




## Base to serialize in changes and in state
def serial_acc(query_accum):
	res = [
		query_accum.accum_id.encode() , # First is the id
		str(query_accum.last_packet_id).encode() , # Then last packet id
		str(query_accum.packet_tracker.expected_next_packet).encode() , # Then exp next id
		DELIMETER_FIELD.join([str(pck_id).encode() for pck_id in query_accum.packet_tracker.missing_packets]), # Then missing packets.
		str(len(query_accum.groups)).encode(), # Then you have the count of groups...
	]

	if len(query_accum.groups) > 0:


		items_iter = iter(query_accum.groups.items())

		first_key, first_value = next(items_iter)


		key_len = len(first_key)
		value_fields = list(first_value.keys())   # preserve order

		# Add key len info and list of fields..
		res.append(str(key_len).encode())
		res.append(",".join(value_fields).encode())

		## Now add value field types... 
		vl_types = [first_value[field] for field in value_fields]
		vl_types = [type(vl).__name__ for vl in vl_types]

		# print(f"SERIAL GRPS {first_value}, {vl_types}\n--------------")
		# print(f"FROM {query_accum.groups}\n--------------")

		res.append(",".join(vl_types).encode()) # Append vl types info.
		
		# print("------> GOT FIRST VL_TYPES", vl_types)

		vl_types = [SERIAL_MAP[vl_type] for vl_type in vl_types]

		res.append(
			DELIMETER_FIELD.join([str(key).encode() for key in first_key])+DELIMETER_FIELD+
			DELIMETER_FIELD.join([vl_types[idx](first_value[field]) for idx, field in enumerate(value_fields)])
		)

		res.extend(
			(
			DELIMETER_FIELD.join([str(key).encode() for key in next_key])+ DELIMETER_FIELD + 
			DELIMETER_FIELD.join([vl_types[idx](next_value[field]) for idx, field in enumerate(value_fields)]) for next_key, next_value in items_iter
			)
		)


	return DELIMETER_ROW.join(res)


# Return packet_tracker_info, count_grps, grps_info_start
def deserialize_header(lines):
	# byte_data.split(DELIMETER_ROW)

	# Header fields always the same...
	# id, last packet, exp nex, missing, then groups len
	res= {
		META_LAST_PACKET_ID: int(lines[1].decode()),
		META_NEXT_EXP_PACKET: int(lines[2].decode()),
	}

	if lines[3] == b"": # Empty missing
		res[META_MISSING_PACKETS]= set()
	else: # missing ids sep by ','
		res[META_MISSING_PACKETS]= set(int(missing.decode()) for missing in lines[3].split(DELIMETER_FIELD))

	return res, int(lines[4].decode()), 5


def deserialize_groups(lines, trg_acc):
	key_len = int(lines[0].decode())
	value_fields = list(field.decode() for field in lines[1].split(DELIMETER_FIELD))
	vl_types = list(DESERIAL_MAP[field.decode()] for field in lines[2].split(DELIMETER_FIELD))

	for row in lines[3:]:
		row = row.decode().split(",")
		grp_key = tuple(row[:key_len])
		acc_grp = {}
		ind=0
		for key,value in zip(value_fields, row[key_len:]):
			acc_grp[key] = vl_types[ind](value)
			ind+=1
		trg_acc[grp_key] = acc_grp






## State is :dict
class BinaryGroupbyStateManager:
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

		# print(f"STR STATE TO DESERIAL? \n'{state.decode()}'\n-------------")

		state = state.split(DELIMETER_ROW)

		acc_id = state[0].decode() # Always first...

		query_accum = self.get_accumulator(acc_id)


		header, count_grps, start_grps = deserialize_header(state)

		query_accum.packet_tracker.expected_next_packet = header[META_NEXT_EXP_PACKET]
		query_accum.packet_tracker.missing_packets = header[META_MISSING_PACKETS]
		query_accum.last_packet_id= header[META_LAST_PACKET_ID]

		if count_grps > 0:
			state = state[start_grps:] # Trim lines
			deserialize_groups(state, query_accum.groups)

		# print(f"STATE? {header} count_grps:{count_grps}\n acc: {query_accum.groups}\n--------------")

		return query_accum


	def deserialize_changes(self, changes_bytes):
		# print(f"STR CHANGEs TO DESERIAL? \n'{changes_bytes.decode()}'\n--------------")
		changes_bytes = changes_bytes.split(DELIMETER_ROW)
		
		header, count_grps, start_grps  = deserialize_header(changes_bytes)
		header[NEW_GRP_ACC] = {}

		batch_ver_count = int(changes_bytes[-1].decode())

		if count_grps > 0:
			changes_bytes = changes_bytes[start_grps:-1] # Trim lines
			deserialize_groups(changes_bytes, header[NEW_GRP_ACC])

		# print(f"CHANGES? {header} count_grps:{count_grps}\n acc: {header[NEW_GRP_ACC]}\n--------------")

		return header, batch_ver_count

	## Almost the same as state... i.e is basically like a snapshot... but no acum id needed.
	def serialize_changes(self, query_accum):
		## Changes when serializing ... is ... in fact... also QueryAccumulator 
		return serial_acc(query_accum) + DELIMETER_ROW + str(query_accum.batch_ver_count).encode()

	# The same because of how groupbynode works...
	def serialize_state(self, query_accum): # Serialize Query accum to metadata and groups and so on
		return serial_acc(query_accum)

	def serialize_initial_state(self, query_accum):
		return serial_acc(query_accum)
