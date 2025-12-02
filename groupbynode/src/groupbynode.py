#from .type_config import TypeConfiguration
import logging
from .groupby_state_manager import GroupbyStateManager, QueryAccumulator

log_info = logging.info
# log_info = print





from common.state_storage.nothing_state_storage import  NothingQueryStateStorage
# from common.state_storage.query_state_storage import  QueryStateStorage
# DEF_STORE_PATH = "/etc/node_state"
from middleware.routing.header_fields import BaseHeaders
from middleware.routing import batch_ack_actions as batch_actions


def get_credentials(accum_id):
	parts = accum_id.split(".")

	return ".".join(parts[:-1]), parts[-1]

class GroupbyNode:
	def __init__(self, group_middleware, payload_deserializer, types_confs, store_creator = NothingQueryStateStorage, batch_size = 1):
		self.middleware = group_middleware;
		self.payload_deserializer = payload_deserializer

		self.types_configurations = types_confs
		self.accumulators = {}

		# This does not load anything yet.. at start we do.
		self.state_storage = store_creator(GroupbyStateManager(self.get_accumulator)) # Have it hardcoded for now
		self.batch_size = batch_size

	def get_acc_id(self, query_id, q_type):
		return f"{query_id}.{q_type}"

	def get_partial_result_from(self, curr_state):
		return curr_state.get_partial_result()

	def get_accumulator(self, accum_id):
		acc = self.accumulators.get(accum_id, None)
		if acc == None:

			query_id, q_type = get_credentials(accum_id)
			new_headers = BaseHeaders(ids = [query_id], types= [q_type])
			log_info(f"Get new accumulator initialization for id {accum_id}, src type {q_type}")


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

	##
	### Handle task for: Batched Blocking Manager on rabbitmq channel version.. requires either return None to just ack this msg and no grouping/batch logic
	### Or to return (ack_group, ack_act) .. as specified on middleware/routing/batch_ack_actions.py
	##

	def handle_task(self, headers, msg):
		query_headers = headers.first_query() # Should only be one for groupby/topk
		q_type = query_headers.types[0]
		accum_id = query_headers.ids[0]+"."+q_type

		if self.state_storage.is_cancelled_query(accum_id):
			log_info(f"Query '{accum_id}' type: {q_type}, was cancelled so ignore message")
			return


		if headers.is_eof(): # Partition EOF is sent when no more data on partition, or when real EOF or error happened as signal.

			if headers.is_error():
				self.propagate_signal(headers)
				acc = self.accumulators.pop(accum_id, None)
				
				if acc == None:
					log_info(f"Query '{accum_id}' type: {q_type}, Error: {headers.get_error_code()}, cancelled query, was not present on state.")
					self.state_storage.cancel_query(accum_id)
					return None # This does auto ack for this msg only... since no acc found, then its impossible

				log_info(f"Query '{accum_id}' type: {q_type}, Error: {headers.get_error_code()}, cancelled query, freeing current state.")

				# Clean up state!
				self.state_storage.backup_query_final(accum_id, acc.version_id , acc) # For debugging/final reviewing purposes
				self.state_storage.unregister_query(accum_id)
				
				# Do it at the end the cancel so that If it crashes in the middle it wont ignore the need to free space.
				self.state_storage.cancel_query(accum_id)

				return (accum_id, batch_actions.FINISH_ACTION)
	
			acc = self.accumulators.get(accum_id, None)

			if acc == None:
				log_info(f"Query '{accum_id}' type: {q_type}, EOF received, EOF was the first message to be received")
				
				config = self.types_configurations[q_type]
				acc = QueryAccumulator(accum_id, config, config.new_builder_for(query_headers))

				self.accumulators[accum_id] = acc

				# May not always be needed but for debugging and descriptability.
				self.state_storage.register_query(accum_id, acc)

			if acc.check_eof(headers.packet_id):
				acc.send_built()
				self.state_storage.backup_query_final(accum_id, acc.version_id , acc)

				log_info(f"Query '{accum_id}' type: {q_type}, EOF received, query finished {accum_id}, freeing state")

				self.state_storage.unregister_query(accum_id)
				del self.accumulators[accum_id] # Remove it


				return (accum_id, batch_actions.FINISH_ACTION) # IF this was actually the EOF finish

			#Else just commit batch even If not on batch count and ACK
			# acc.batch_ver_count+=1  # Not needed to sum ? since ack message is last and is saved as exp ? 
			self.state_storage.write_changes(accum_id, acc.version_id, acc) # Save state
			self.state_storage.commit_changes(accum_id)
			self.state_storage.push_changes(accum_id, acc.version_id, acc, acc.batch_ver_count)
			acc.batch_ver_count = 0

			return (accum_id, batch_actions.ACK_ACTION) # Ack batch messages

			
		acc = self.accumulators.get(accum_id, None)
		if acc == None:
			log_info(f"Query '{accum_id}' type: {q_type}, Accumulator initialization")
			config = self.types_configurations[q_type]
			acc = QueryAccumulator(accum_id, config, config.new_builder_for(query_headers))

			self.accumulators[accum_id] = acc

			self.state_storage.register_query(accum_id, acc)

		if acc.is_duplicated(query_headers.packet_id):
			log_info(f"Query: '{accum_id}' found duplicated {query_headers.packet_id}")
			return # Return None just ack this message as stand alone


		msg = self.payload_deserializer(msg)

		for row in msg.stream_rows():
			acc.check(row)

		if acc.add_msg(query_headers.packet_id):
			log_info(f"Query '{accum_id}' type: {q_type}, received last version {acc.version_id}. Finishing.")
			acc.send_built()

			self.state_storage.backup_query_final(accum_id, acc.version_id , acc)
			self.state_storage.unregister_query(accum_id)

			del self.accumulators[accum_id]
			return (accum_id, batch_actions.FINISH_ACTION)

		if acc.batch_ver_count >= self.batch_size:
			# Only save/push on batch size count.
			
			self.state_storage.write_changes(accum_id, acc.version_id, acc) # Save state
			self.state_storage.commit_changes(accum_id)
			self.state_storage.push_changes(accum_id, acc.version_id, acc, acc.batch_ver_count)
			acc.batch_ver_count = 0

			return (accum_id, batch_actions.ACK_ACTION) # Ack batch messages

		return (accum_id, batch_actions.ACCUMULATE_ACTION)


	def start(self):
		# Even If there is no pending changes, load states of queries, to continue on memory and not load always from disk.
		for accum_id, (version_id, state) in self.state_storage.load_states().items():
			log_info(f"At start restored query: {accum_id}, last version:{version_id}")
			state.version_id = version_id # To have it in mind for future messages/handling

		self.state_storage.check_integrity()
		

		self.middleware.start_consuming(self.handle_task)		

	def close(self):
		self.middleware.close()
		for k, conf in self.types_configurations.items():
			conf.close()