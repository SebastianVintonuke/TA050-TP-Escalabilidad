#from .type_config import TypeConfiguration
import logging
from .groupby_state_manager import GroupbyStateManager, QueryAccumulator

from common.state_storage.nothing_state_storage import  NothingQueryStateStorage
# from common.state_storage.query_state_storage import  QueryStateStorage
# DEF_STORE_PATH = "/etc/node_state"
from middleware.routing.header_fields import BaseHeaders
def get_credentials(accum_id):
	parts = accum_id.split("_")

	return "_".join(parts[:-1]), parts[-1]

class GroupbyNode:
	def __init__(self, group_middleware, payload_deserializer, types_confs, store_creator = NothingQueryStateStorage, batch_size = 1):
		self.middleware = group_middleware;
		self.payload_deserializer = payload_deserializer

		self.types_configurations = types_confs
		self.accumulators = {}

		# This does not load anything yet.. at start we do.
		self.state_storage = store_creator(GroupbyStateManager(self.get_accumulator)) # Have it hardcoded for now
		self.batch_size = batch_size


	def get_accumulator(self, accum_id):
		acc = self.accumulators.get(accum_id, None)
		if acc == None:

			query_id, q_type = get_credentials(accum_id)
			new_headers = BaseHeaders(ids = [query_id], types= [q_type])
			logging.info(f"Get new accumulator initialization for id {accum_id}, src type {q_type}")


			config = self.types_configurations[q_type]
			acc = QueryAccumulator(accum_id, config, config.new_builder_for(new_headers))

			self.accumulators[accum_id] = acc		
		return acc

	def propagate_signal(self, headers):
		for prop_headers in headers.split():
			conf = self.types_configurations[prop_headers.types[0]]
			conf.send(
				conf.new_builder_for(prop_headers) #Empty message that has same headers splitting to each destination.
			)

	def len_in_progress(self):
		return len(self.accumulators)

	def len_total_groups(self):
		total = 0
		for _, vl in self.accumulators.items():
			total+= vl.len_grouped()

		return total

	def backup_acc(self, acc):
		self.state_storage.write_changes(acc.accum_id, acc.messages_received, acc) # Save state
		self.state_storage.commit_changes(acc.accum_id)
		self.state_storage.push_changes(acc.accum_id, acc.messages_received, acc, acc.batch_msg_count)


	def handle_task(self, headers, msg):
		if headers.is_eof(): # Partition EOF is sent when no more data on partition, or when real EOF or error happened as signal.
			if headers.is_error():
				logging.info(f"Received ERROR code: {headers.get_error_code()} IN {headers.ids} | type: {headers.types}")
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

					# May not always be needed but for debugging and descriptability.
					self.state_storage.register_query(accum_id, acc)

				if acc.check_eof(headers.msg_count):
					self.backup_acc(acc)
					acc.send_built()
					del self.accumulators[accum_id] # Remove it

			return # This does auto ack since for now its stateless..
			
		msg = self.payload_deserializer(msg)
		query_headers = headers.first_query() # Should only be one for groupby/topk

		q_type = query_headers.types[0]
		accum_id = query_headers.ids[0]+"_"+q_type

		acc = self.accumulators.get(accum_id, None)
		if acc == None:
			logging.info(f"New accumulator initialization for {query_headers.ids[0]}, type {q_type}")
			config = self.types_configurations[q_type]
			acc = QueryAccumulator(accum_id, config, config.new_builder_for(query_headers))

			self.accumulators[accum_id] = acc
			self.state_storage.register_query(accum_id, acc)

		for row in msg.stream_rows():
			acc.check(row)

		if acc.add_msg_count():
			logging.info(f"query: {query_headers.ids[0]} type: {q_type}, received last messasge {acc.messages_received} >= {acc.known_message_len}. Start sending.")
			
			self.backup_acc(acc)
			
			acc.send_built()
			del self.accumulators[acc.accum_id]
			return # Ack batch 


		# Not yet taking into account dups
		acc.batch_msg_count+=1 

		if acc.batch_msg_count >= self.batch_size:
			# Only save/push on batch size count.
			
			self.state_storage.write_changes(acc.accum_id, acc.messages_received, acc) # Save state
			self.state_storage.commit_changes(acc.accum_id)
			self.state_storage.push_changes(acc.accum_id, acc.messages_received, acc, acc.batch_msg_count)
			acc.batch_msg_count = 0

			return # Ack batch messages

		return True # Return true to accumulate in batch.


	def start(self):
		# Even If there is no pending changes, load states of queries, to continue on memory and not load always from disk.
		self.state_storage.load_states() 
		self.state_storage.check_integrity()
		

		self.middleware.start_consuming(self.handle_task)		

	def close(self):
		self.middleware.close()
		for k, conf in self.types_configurations.items():
			conf.close()