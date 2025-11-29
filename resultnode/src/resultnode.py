import logging
from typing import List

from common.middleware.middleware import MessageMiddlewareQueue
from .resultnode_state_handler import UserQueryCounter, ResultNodeStateManager

from common.utils import stable_hash
from common.state_storage.nothing_state_storage import  NothingQueryStateStorage
log_info = logging.info
def get_credentials(accum_id):
	parts = accum_id.split("_")

	return "_".join(parts[:-1]), parts[-1]


class ResultNode:
	def __init__(self, in_middle, out_middle: List[MessageMiddlewareQueue], payload_deserializer, handlers, store_creator = NothingQueryStateStorage):
		self.results_message_counter= {}
		self.in_middle = in_middle
		self.out_middle: List[MessageMiddlewareQueue] = out_middle
		self.payload_deserializer = payload_deserializer
		self.map_handlers = handlers

		# This does not load anything yet.. at start we do.
		self.state_storage = store_creator(ResultNodeStateManager(self.get_counter)) # Have it hardcoded for now


	def get_counter(self, user_query_id: str):
		counter = self.results_message_counter.get(user_query_id, None)
		if counter == None:

			user_id, query_type = get_credentials(user_query_id)
			handler = self.map_handlers.get(query_type, None)
			log_info(f"Initializing user query counter for {user_id}, type: {query_type}")

			# If its invalid i.e handler == None .. its discarded on start after loading
			counter = UserQueryCounter(user_query_id, handler)

			self.results_message_counter[user_query_id] = counter

		return counter


	def handle_result(self, headers, msg):
		user_id = headers.ids[0]
		query_type = headers.types[0]
		user_query_id = user_id+"_"+query_type


		if self.state_storage.is_cancelled_query(user_query_id):
			log_info(f"Query '{user_query_id}' type: {query_type}, was cancelled so ignore message")
			return


		out_middle = self.out_middle_for(user_id);
		
		if headers.is_error():
			log_info(f"Query '{user_query_id}' Error: {headers.get_error_code()}, cancelled query, freeing saved state.")
			counter = self.results_message_counter.pop(user_query_id, None)
			if counter != None:

				# Clean up state!
				self.state_storage.backup_query_final(user_query_id, counter.version_id , counter) # For debugging/final reviewing purposes
				self.state_storage.unregister_query(user_query_id)

				handler = counter.handler
			else:
				handler = self.map_handlers.get(query_type, None)

			if handler:
				out_middle.send(handler.get_abort_msg(user_id))


			# Do it at the end the cancel so that If it crashes in the middle it wont ignore the need to free space.
			self.state_storage.cancel_query(user_query_id)
			return

		counter = self.results_message_counter.get(user_query_id, None)

		if counter == None:
			handler = self.map_handlers.get(query_type, None)

			if handler == None:
				log_info(f"Error invalid user query counter for {user_id}, type: {query_type}")
				return

			log_info(f"Initializing user query counter for {user_id}, type: {query_type}")
			counter = UserQueryCounter(user_query_id, handler)
			self.results_message_counter[user_query_id] = counter
			self.state_storage.register_query(user_query_id, counter)
		else:
			handler = counter.handler


		if counter.packet_tracker.is_duplicate(headers.packet_id):
			log_info(f"Received duplicate message count for '{user_query_id}', {headers.packet_id}")
			return
		counter.packet_tracker.check_new_packet(headers.packet_id)

		if headers.is_eof():
			counter.last_packet_id = headers.packet_id
			log_info(f"Received expected message count for '{user_query_id}', last packet is {headers.packet_id} missing: {counter.packet_tracker.missing_packets}")
		else:
			msg = self.payload_deserializer(msg)
			out_middle.send(handler.handle_new_results(msg,  user_id))
		counter.version_id +=1 # Add 1 to version even on final eof or so

		if counter.is_eof():
			log_info(f"Query '{user_query_id}' finished last: {headers.packet_id}, freeing saved state.")
			out_middle.send(handler.get_eof_msg(user_id))

			# If it was actually the eof... then unregister
			self.state_storage.backup_query_final(user_query_id, counter.version_id , counter)
			self.state_storage.unregister_query(user_query_id)
			del self.results_message_counter[user_query_id]
			return

		self.state_storage.write_changes(user_query_id, counter.version_id, counter) # Save state
		self.state_storage.commit_changes(user_query_id)
		self.state_storage.push_changes(user_query_id, counter.version_id, counter, counter.batch_ver_count)

	def out_middle_for(self, client_id: str) -> MessageMiddlewareQueue:
		index = stable_hash(client_id) % len(self.out_middle)
		return self.out_middle[index]

	def start(self):
		# Even If there is no pending changes, load states of queries, to continue on memory and not load always from disk.
		for user_query_id, (version_id, counter) in self.state_storage.load_states().items():
			log_info(f"At start restored query: {user_query_id}, last version:{version_id}")
			counter.version_id = version_id # To have it in mind for future messages/handling
		
		self.state_storage.check_integrity()

		self.in_middle.start_consuming(self.handle_result)

	def close(self):
		self.in_middle.close()
		for out_middle in self.out_middle:
			out_middle.close()



"""
		result_task= handler.handle_new_results(headers, msg, counter,  user_id)
		if result_task != None:
			self.out_middle_for(user_id).send(result_task)

		handler.check_send_eof(counter, user_id, self.out_middle_for(user_id))
		#raise ValueError(f"Unknown query type: {query_type}")


"""