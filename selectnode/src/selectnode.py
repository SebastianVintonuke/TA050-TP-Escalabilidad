#from .type_config import TypeConfiguration

class TypeHandler:
	def __init__(self, type_conf, msg_builder):
		self.type_conf = type_conf
		self.msg_builder = msg_builder

	def __init__(self, type_conf, msg, ind):
		self.type_conf = type_conf
		self.msg_builder = type_conf.new_builder_for(msg, ind)

	def check(self, row):
		if self.type_conf.should_keep(row):
			self.msg_builder.add_row(row)

	def send_built(self):
		self.type_conf.send(self.msg_builder)

class SelectNode:
	def __init__(self, select_middleware, types_confs):
		self.middleware = select_middleware;
		self.types_configurations = types_confs

	def handle_task(self, msg):
		msg.describe()

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