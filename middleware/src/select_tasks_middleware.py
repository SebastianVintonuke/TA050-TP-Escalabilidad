from .rabbitmq_middleware import *
from . import routing
from .routing.select_task_message import *

SELECT_TASKS_QUEUE_BASE = "select_tasks_queue"

class SelectTasksMiddleware(RabbitQueueMiddleware):
	def __init__(self, host = routing.RABBITMQ_HOST, queue_name = SELECT_TASKS_QUEUE_BASE):
		super().__init__(queue_name, host)


	def _callback_wrapper(self, callback):
		def real_callback(ch, method, properties, body):
			logging.info(f"action: select_rcv_msg | result: success | method: {method} | props: {properties} | body:{body}")
			
			# Wrap specific message
			wrapped_msg = SelectTaskMessage(ch, method, properties.headers, body)

			return callback(wrapped_msg)
		return real_callback
