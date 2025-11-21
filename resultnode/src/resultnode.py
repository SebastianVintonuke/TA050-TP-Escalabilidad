import logging

class Counter:
	def __init__(self):
		self.count_query_1 = 0
		self.count_query_2_profit = 0
		self.count_query_2_quantity = 0
		self.count_query_3 = 0
		self.count_query_4 = 0
		self.expected_count_query_1 = -1
		self.expected_count_query_2_profit = -1
		self.expected_count_query_2_quantity = -1
		self.expected_count_query_3 = -1
		self.expected_count_query_4 = -1

	def is_eof_q1(self):
		return self.expected_count_query_1 >=0 and self.count_query_1 >= self.expected_count_query_1
	def is_eof_q2_profit(self):
		return self.expected_count_query_2_profit >=0 and self.count_query_2_profit >= self.expected_count_query_2_profit
	def is_eof_q2_quantity(self):
		return self.expected_count_query_2_quantity >=0 and self.count_query_2_quantity >= self.expected_count_query_2_quantity
	
	def is_eof_q3(self):
		return self.expected_count_query_3 >=0 and self.count_query_3 >= self.expected_count_query_3
	def is_eof_q4(self):
		return self.expected_count_query_4 >=0 and self.count_query_4 >= self.expected_count_query_4

class ResultNode:
	def __init__(self, in_middle, out_middle, payload_deserializer, handlers):
		self.results_message_counter= {}
		self.in_middle = in_middle
		self.out_middle = out_middle
		self.payload_deserializer = payload_deserializer
		self.map_handlers = handlers

	
	def get_counter(self, user_id: str):
		counter = self.results_message_counter.get(user_id, None)
		if counter == None:
			counter = Counter()
			self.results_message_counter[user_id] = counter

		return counter   


	def handle_result(self, headers, msg):
		user_id = headers.ids[0]
		query_type = headers.types[0]
		msg = self.payload_deserializer(msg)
		counter = self.get_counter(user_id)

		handler = self.map_handlers.get(query_type, None)


		if handler:
			result_task= handler.handle_new_results(headers, msg, counter,  user_id)
			if result_task != None:
				self.out_middle.send(result_task)
			
			handler.check_send_eof(counter, user_id, self.out_middle)
		else:
			logging.info(f"NO EXISTE {headers.types}")
			#raise ValueError(f"Unknown query type: {query_type}")



	def start(self):
		self.in_middle.start_consuming(self.handle_result)

	def close(self):
		self.in_middle.close()
		self.out_middle.close()

