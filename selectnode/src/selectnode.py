#from .type_config import TypeConfiguration
import logging
class TypeHandler:
	def __init__(self, type_conf, msg_builder):
		self.type_conf = type_conf
		self.msg_builder = msg_builder

	def __init__(self, type_conf, msg, ind):
		self.type_conf = type_conf
		self.msg_builder = type_conf.new_builder_for(msg, ind)

	def check(self, row):
		row = self.type_conf.filter_map(row)
		if row != None:
			self.msg_builder.add_row(row)

	def send_built(self):
		self.type_conf.send(self.msg_builder)

class SelectNode:
	def __init__(self, select_middleware, types_confs):
		self.middleware = select_middleware;
		self.types_configurations = types_confs

	def handle_task(self, msg):
		msg.describe()

		if msg.is_partition_eof(): # Partition EOF is sent when no more data on partition, or when real EOF or error happened as signal.
			for ind in range(msg.len_queries()):
				conf = self.types_configurations[msg.types[ind]]
				self.type_conf.send(
					conf.new_builder_for(msg, ind) #Empty message that has same headers splitting to each destination.
				)
			msg.ack_self()
			return
			#logging.info(f"Should handle EOF, or error in send, code {msg.partition}")

		outputs = []
		ind = 0
		for type in msg.types:
			outputs.append(
				TypeHandler(self.types_configurations[type], msg, ind))
			ind+=1

		for row in msg.stream_rows():
			for output in outputs:
				output.check(row)

		for output in outputs:
			output.send_built()

		msg.ack_self()

	def start(self):
		self.middleware.start_consuming(self.handle_task)		

	def close(self):
		self.middleware.close()
		for k, conf in self.types_configurations.items():
			conf.close()