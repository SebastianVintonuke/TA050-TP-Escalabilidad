

from .row_filtering import load_all_filters
class SelectNode:
	def __init__(self, middleware_conn, types_confs):
		self.channel = middleware_conn.open_channel();
		self.channel.consume_select_tasks(self.handle_task)
		self.filters = {}

		for type_conf in types_confs:
			self.filters[type_conf.type] = load_all_filters(type_conf.filters)

	def handle_task(self, msg):
		msg.describe()

		outputs = []
		for i in range(msg.len_queries()):
			outputs.append([])




		msg.stream_rows()
		msg.ack_self()

	def start(self):
		self.channel.start_consume()