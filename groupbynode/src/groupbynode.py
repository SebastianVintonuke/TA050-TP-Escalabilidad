#from .type_config import TypeConfiguration
import logging



class QueryAccumulator:
	def __init__(self, type_conf, msg_builder):
		self.type_conf = type_conf
		self.msg_builder = msg_builder
		self.ongoing_partitions = set()
		self.groups = {}

	def __init__(self, type_conf, msg, ind):
		self.type_conf = type_conf
		self.msg_builder = type_conf.new_builder_for(msg, ind)
		
		self.ongoing_partitions = set()
		self.groups = {}

	def check(self, row):
		row = self.type_conf.map_input_row(row)
		key = self.type_conf.key_parser.get_group_key(row)

		acc = self.groups.get(key, None)
		if acc == None:
			acc = self.type_conf.grouper.new_group_acc(row)
			self.groups[key] = acc
		else:
			self.type_conf.grouper.add_group_acc(acc, row)
			#acc.add_row(row) # Better in design? who knows

	def send_built(self): # What happens If the groupbynode fails here/shutdowns here?
		for group, acc in self.groups.items():
			self.type_conf.add_output(self.msg_builder, group, acc)

		self.type_conf.send(self.msg_builder)
		eof_signal = self.msg_builder.clone()
		eof_signal.set_as_eof()
		self.type_conf.send(eof_signal)
		eof_signal.set_finish_signal()
		self.type_conf.send(eof_signal)

	def len_grouped(self):
		return len(self.groups)

	def describe(self):
		if len(self.groups) < 100:
			logging.info(f"curr status accumulator topk len {self.groups}:")
			for group, acc in self.groups.items():
				logging.info(f"key {group} acc : {acc}")

class GroupbyNode:
	def __init__(self, group_middleware, types_confs):
		self.middleware = group_middleware;
		self.types_configurations = types_confs
		self.accumulators = {}


	def propagate_signal(self, msg):
		for ind in range(msg.len_queries()):
			conf = self.types_configurations[msg.types[ind]]
			self.type_conf.send(
				conf.new_builder_for(msg, ind) #Empty message that has same headers splitting to each destination.
			)

	def len_in_progress(self):
		return len(self.accumulators)

	def len_total_groups(self):
		total = 0
		for _, vl in self.accumulators.items():
			total+= vl.len_grouped()

		return total

	def handle_task(self, msg):
		if msg.is_partition_eof(): # Partition EOF is sent when no more data on partition, or when real EOF or error happened as signal.
			if msg.is_last_message():
				if msg.is_eof():
					logging.info(f"Received final eof OF {msg.ids} types: {msg.types}")
					ind=0
					for query_id in msg.ids:
						query_id = query_id+str(msg.types[ind])
						acc = self.accumulators.get(query_id, None)
						#logging.info(f"ACC: {acc}")
						if acc:
							acc.send_built()
							del self.accumulators[query_id] #Remove it
						else:
							# propagate eof signal for this message 
							conf = self.types_configurations[msg.types[ind]]
							self.type_conf.send(
								conf.new_builder_for(msg, ind) #Empty message that has same headers splitting to each destination.
							)

						ind+=1
				else:
					logging.info(f"Received ERROR code: {msg.partition} IN {msg.ids}")
					self.propagate_signal(msg)
			else: # Not last message, mark partition as ended
				ind = 0
				for query_id in msg.ids:
					query_id = query_id+str(msg.types[ind])
					acc = self.accumulators.get(query_id, None)
					if acc:
						acc.ongoing_partitions.discard(msg.partition)
					ind +=1
			return

		outputs = []
		ind = 0
		for query_id in msg.ids:
			query_id = query_id+str(msg.types[ind]) # Change it take into account the type so we can do multiple queries at once
			acc = self.accumulators.get(query_id, None)

			if acc == None:
				acc = QueryAccumulator(self.types_configurations[msg.types[ind]], msg, ind)
				self.accumulators[query_id] = acc
			
			outputs.append(acc)

			ind+=1

		for row in msg.stream_rows():
			for output in outputs:
				output.check(row)

		#for output in outputs:
		#	output.describe()


	def start(self):
		self.middleware.start_consuming(self.handle_task)		

	def close(self):
		self.middleware.close()
		for k, conf in self.types_configurations.items():
			conf.close()