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
		key = self.type_conf.grouper.get_group_key(row)

		acc = self.groups.get(key, None)
		if acc == None:
			acc = self.type_conf.grouper.new_group_acc(row)
			self.groups[key] = acc
		else:
			self.type_conf.grouper.add_group_acc(acc, row)
			#acc.add_row(row) # Better in design? who knows

	def send_built(self): # What happens If the groupbynode fails here/shutdowns here?
		for group, acc in self.groups.items():
			self.msg_builder.add_row(self.type_conf.get_output(group, acc))

		self.type_conf.send(self.msg_builder)

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


	def handle_task(self, msg):
		msg.describe()

		if msg.is_partition_eof(): # Partition EOF is sent when no more data on partition, or when real EOF or error happened as signal.
			if msg.is_last_message():
				if msg.is_eof():
					logging.info(f"Received final eof OF {msg.ids}")
					for query_id in msg.ids:
						acc = self.accumulators.get(query_id, None)
						if acc:
							acc.send_built()
					#self.propagate_signal(msg)
				else:
					logging.info(f"Received ERROR code: {msg.partition} IN {msg.ids}")
					self.propagate_signal(msg)
			else: # Not last message, mark partition as ended
				for query_id in msg.ids:
					acc = self.accumulators.get(query_id, None)
					if acc:
						acc.ongoing_partitions.discard(msg.partition)
			msg.ack_self()
			return


		outputs = []
		ind = 0
		for query_id in msg.ids:
			acc = self.accumulators.get(query_id, None)

			if acc == None:
				acc = QueryAccumulator(self.types_configurations[msg.types[ind]], msg, ind)
				self.accumulators[query_id] = acc
			
			outputs.append(acc)

			ind+=1

		for row in msg.stream_rows():
			for output in outputs:
				output.check(row)

		msg.ack_self()

	def start(self):
		self.middleware.start_consuming(self.handle_task)		

	def close(self):
		self.middleware.close()
		for k, conf in self.types_configurations.items():
			conf.close()