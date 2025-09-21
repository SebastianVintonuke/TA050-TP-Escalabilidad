from common import routing 

class SelectNode:
	def __init__(self, middleware_conn: routing.Connection ):
		self.channel = middleware_conn.open_channel();
		self.channel.consume_select_tasks(self.handle_task)

	def handle_task(self, task):
		pass

	
	def start(self):
		self.channel.start_consume()