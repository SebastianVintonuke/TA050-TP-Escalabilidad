

from .row_filtering import load_all_filters, should_keep


class TypeConfiguration:
	def __init__(self, type_conf):
		self.filters = load_all_filters(type_conf)

	def should_keep(self, row):
		return should_keep(self.filters, row)


class TypeHandler:
	def __init__(self, type_conf, msg_builder):
		self.type_conf = type_conf
		self.msg_builder = msg_builder

	def check(self, row):
		if self.type_conf.should_keep(row):
			self.msg_builder.add_row(row)

	def send_to(self, sender):
		sender.send_msg(self.msg_builder.build())

class SelectNode:
	def __init__(self, middleware_conn, types_confs):
		self.channel = middleware_conn.open_channel();
		self.channel.consume_select_tasks(self.handle_task)
		self.types_configurations = {}

		for q_type, type_conf in types_confs.items():
			self.types_configurations[q_type] = TypeConfiguration(type_conf)

		# FOR now just results sending!
		self.results_sender = self.channel.open_sender_to_results()

	def handle_task(self, msg):
		msg.describe()

		outputs = []
		ind = 0
		for type in msg.types:
			outputs.append(TypeHandler(
				self.types_configurations[type],
				msg.result_builder_for_single(ind))
			)
			ind+=1

		for row in msg.stream_rows():
			for output in outputs:
				output.check(row)

		for output in outputs:
			output.send_to(self.results_sender)

		msg.ack_self()

	def start(self):
		self.channel.start_consume()