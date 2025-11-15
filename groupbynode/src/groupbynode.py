#from .type_config import TypeConfiguration
import logging
from .groupby_state_manager import GroupbyStateManager

from common.state_storage.nothing_state_storage import  NothingQueryStateStorage
# from common.state_storage.query_state_storage import  QueryStateStorage
# DEF_STORE_PATH = "/etc/node_state"


class QueryAccumulator:
	def __init__(self, accum_id, type_conf, msg_builder):
		self.type_conf = type_conf
		self.accum_id = accum_id
		self.msg_builder = msg_builder
		self.groups = {}
		self.messages_tags = []
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

class GroupbyNode:
	def __init__(self, group_middleware, payload_deserializer, types_confs, store_creator = NothingQueryStateStorage):
		self.middleware = group_middleware;
		self.payload_deserializer = payload_deserializer

		self.types_configurations = types_confs
		self.accumulators = {}

		# This does not load anything yet.. at start we do.
		self.state_storage = store_creator(GroupbyStateManager(self.get_accumulator)) # Have it hardcoded for now


	def get_accumulator(self, accum_id):
		acc = self.accumulators.get(accum_id, None)
		if acc == None:
			q_type = accum_id.split("_")[-1]
			logging.info(f"Get new accumulator initialization for id {accum_id}, type {q_type}")


			config = self.types_configurations[q_type]
			acc = QueryAccumulator(accum_id, config, config.new_builder_for(new_headers))

			self.accumulators[accum_id] = acc		

	def propagate_signal(self, headers):
		for prop_headers in headers.split():
			conf = self.types_configurations[prop_headers.types[0]]
			self.type_conf.send(
				conf.new_builder_for(prop_headers) #Empty message that has same headers splitting to each destination.
			)

	def len_in_progress(self):
		return len(self.accumulators)

	def len_total_groups(self):
		total = 0
		for _, vl in self.accumulators.items():
			total+= vl.len_grouped()

		return total

	def handle_task(self, headers, msg):
		if headers.is_eof(): # Partition EOF is sent when no more data on partition, or when real EOF or error happened as signal.
			if headers.is_error():
				logging.info(f"Received ERROR code: {headers.get_error_code()} IN {headers.ids}")
				self.propagate_signal(headers)
				return # This does auto ack since for now its stateless.. should remove state though

			logging.info(f"Received final eof OF {headers.ids} types: {headers.types}, should have been {headers.msg_count} messages")
			for new_headers in headers.split():
				q_type = new_headers.types[0]
				accum_id = new_headers.ids[0]+"_"+q_type

				acc = self.accumulators.get(accum_id, None)
				if acc == None:
					logging.info(f"For type {q_type}, eof was the first message to be received")
					
					config = self.types_configurations[q_type]
					acc = QueryAccumulator(accum_id, config, config.new_builder_for(new_headers))

					self.accumulators[accum_id] = acc

				if acc.check_eof(headers.msg_count):
					acc.send_built()
					del self.accumulators[accum_id] # Remove it
				else:
					self.state_storage.register_query(accum_id, acc)

			return # This does auto ack since for now its stateless..
			
		msg = self.payload_deserializer(msg)
		outputs = []
		for new_headers in headers.split():
			q_type = new_headers.types[0]
			accum_id = new_headers.ids[0]+"_"+q_type

			acc = self.accumulators.get(accum_id, None)
			if acc == None:
				logging.info(f"New accumulator initialization for {new_headers.ids[0]}, type {q_type}")
				config = self.types_configurations[q_type]
				acc = QueryAccumulator(accum_id, config, config.new_builder_for(new_headers))

				self.accumulators[accum_id] = acc
				self.state_storage.register_query(accum_id, acc)
			outputs.append(acc)

		for row in msg.stream_rows():
			for output in outputs:
				output.check(row)

		for ind, acc in enumerate(outputs):
			if acc.add_msg_count():
				logging.info(f"query: {headers.ids[ind]} type: {headers.types[ind]}, received last messasge {acc.messages_received} >= {acc.known_message_len}. Start sending.")
				acc.send_built()
				del self.accumulators[acc.accum_id]
			else:

				#acc.messages_tags.append(headers.tag)
				# Packet id should be eq to acc.messages_received, since order does not matter as much?

				self.state_storage.write_changes(acc.accum_id, acc.messages_received, acc) # Save state
				self.state_storage.commit_changes(acc.accum_id)
				self.state_storage.push_changes(acc.accum_id, acc.messages_received, acc, acc.messages_tags)

				self.state_storage.ack_finished(self.middleware.ack_message)

		return False # Do not auto ack



	def start(self):
		self.state_storage.check_integrity()
		self.state_storage.ack_finished(self.middleware.ack_message)
		self.middleware.start_consuming(self.handle_task)		

	def close(self):
		self.middleware.close()
		for k, conf in self.types_configurations.items():
			conf.close()