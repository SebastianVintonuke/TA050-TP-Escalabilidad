import logging
from typing import List

from common.middleware.middleware import MessageMiddlewareQueue
from common.utils import stable_hash
from .resultnode_state_handler import UserCounter, ResultNodeStateManager
from common.state_storage.nothing_state_storage import  NothingQueryStateStorage

class ResultNode:
	def __init__(self, in_middle, out_middle: List[MessageMiddlewareQueue], payload_deserializer, handlers, store_creator = NothingQueryStateStorage):
		self.results_message_counter= {}
		self.in_middle = in_middle
		self.out_middle: List[MessageMiddlewareQueue] = out_middle
		self.payload_deserializer = payload_deserializer
		self.map_handlers = handlers

		# This does not load anything yet.. at start we do.
		self.state_storage = store_creator(ResultNodeStateManager(self.get_counter)) # Have it hardcoded for now



	def get_counter(self, user_id: str):
		counter = self.results_message_counter.get(user_id, None)
		if counter == None:
			counter = UserCounter(user_id)
			self.results_message_counter[user_id] = counter

		return counter


	def handle_result(self, headers, msg):
		user_id = headers.ids[0]
		query_type = headers.types[0]
		handler = self.map_handlers.get(query_type, None)

		if handler == None:
			logging.info(f"NO EXISTE {headers.types}")
			return

		if headers.is_error():
			logging.info(f"action: abort | result: in-progress | {headers}")
			handler.send_abort(user_id, self.out_middle_for(user_id))
			return

		msg = self.payload_deserializer(msg)
		counter = self.results_message_counter.get(user_id, None)

		if counter == None:
			counter = UserCounter(user_id)
			self.results_message_counter[user_id] = counter

			self.state_storage.register_query(user_id, counter)

		result_task= handler.handle_new_results(headers, msg, counter,  user_id)
		if result_task != None:
			self.out_middle_for(user_id).send(result_task)

		handler.check_send_eof(counter, user_id, self.out_middle_for(user_id))
		#raise ValueError(f"Unknown query type: {query_type}")

		counter.pkt_id_counter +=1 # Add 1 to pckt id counter. Used as pkt id since order does not matter? yeah .. no should not count duplicates! TODO change to use pkt id when in headers!
		self.state_storage.write_changes(user_id, counter.pkt_id_counter, counter) # Save state
		self.state_storage.commit_changes(user_id)
		self.state_storage.push_changes(user_id, counter.pkt_id_counter, counter, counter.batch_msg_count)

	def out_middle_for(self, client_id: str) -> MessageMiddlewareQueue:
		index = stable_hash(client_id) % len(self.out_middle)
		return self.out_middle[index]

	def start(self):
		# Even If there is no pending changes, load states of queries, to continue on memory and not load always from disk.
		self.state_storage.load_states()
		self.state_storage.check_integrity()

		self.in_middle.start_consuming(self.handle_result)

	def close(self):
		self.in_middle.close()
		for out_middle in self.out_middle:
			out_middle.close()

