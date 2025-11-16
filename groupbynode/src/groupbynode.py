#from .type_config import TypeConfiguration
import logging
from .groupby_state_manager import GroupbyStateManager, QueryAccumulator

from common.state_storage.nothing_state_storage import  NothingQueryStateStorage
# from common.state_storage.query_state_storage import  QueryStateStorage
# DEF_STORE_PATH = "/etc/node_state"

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

				acc.messages_tags.append(headers.tag)
				# Packet id should be eq to acc.messages_received, since order does not matter as much?

				self.state_storage.write_changes(acc.accum_id, acc.messages_received, acc) # Save state
				self.state_storage.commit_changes(acc.accum_id)
				self.state_storage.push_changes(acc.accum_id, acc.messages_received, acc, acc.messages_tags)

				self.state_storage.ack_finished(self.middleware.ack_message)

		return False # Do not auto ack



	def start(self):
		self.state_storage.load_states()
		self.state_storage.check_integrity()
		self.state_storage.ack_finished(self.middleware.ack_message)
		self.middleware.start_consuming(self.handle_task)		

	def close(self):
		self.middleware.close()
		for k, conf in self.types_configurations.items():
			conf.close()