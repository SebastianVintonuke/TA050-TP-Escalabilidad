
import logging
from common.state_storage.packet_id_tracker import PacketIDTracker

class QueryAccumulator:
	def __init__(self, accum_id, type_conf, msg_builder):
		self.type_conf = type_conf
		self.accum_id = accum_id
		self.msg_builder = msg_builder
		self.groups = {}
		self.batch_ver_count = 0


		self.packet_tracker =PacketIDTracker()
		self.version_id = 0

		self.last_packet_id= -1
		self.rows_recv = 0
		self.msg_builder.reset_eof() # Ensure its not copying the eof flag from input sender

	def is_duplicated(self, packet_id):
		return self.packet_tracker.is_duplicate(packet_id)
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

	def get_partial_result(self):
		partial = self.msg_builder.clone()

		for group, acc in self.groups.items():
			self.type_conf.add_output(partial, group, acc)

		return partial

	def send_built(self): # What happens If the groupbynode fails here/shutdowns here?
		for group, acc in self.groups.items():
			self.type_conf.add_output(self.msg_builder, group, acc)

		self.msg_builder.headers.packet_id = 0 # Reset packet id for next in line

		logging.info(f"Grouper node sending EOF rows processed {self.rows_recv} msg sent 1")

		self.type_conf.send(self.msg_builder)
		eof_signal = self.msg_builder.clone()
		eof_signal.headers.packet_id = 1 # Set packet id.
		eof_signal.set_as_eof()
		self.type_conf.send(eof_signal)

	def len_grouped(self):
		return len(self.groups)

	def describe(self):
		if len(self.groups) < 100:
			logging.info(f"curr status accumulator topk len {self.groups}:")
			for group, acc in self.groups.items():
				logging.info(f"key {group} acc : {acc}")

	def add_msg(self, packet_id):
		self.version_id += 1 # Inc version by one each msg
		self.batch_ver_count+=1 

		self.packet_tracker.check_new_packet(packet_id)

		return self.last_packet_id>=0 and self.packet_tracker.handled_all_up_to(self.last_packet_id)

	def check_eof(self, eof_packet_id):
		
		if self.last_packet_id >=0: # Already received EOF ... so its a duplicated.
			return self.packet_tracker.handled_all_up_to(eof_packet_id)
			
		self.version_id += 1 # Inc version by one each msg .. even EOF msg
		self.batch_ver_count+=1 

		self.last_packet_id = eof_packet_id

		self.packet_tracker.check_new_packet(eof_packet_id)
		return self.packet_tracker.handled_all_up_to(eof_packet_id)



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