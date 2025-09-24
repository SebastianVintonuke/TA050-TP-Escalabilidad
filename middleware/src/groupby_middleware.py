from .rabbitmq_middleware import *
from . import routing
from .routing.groupby_message import *

GROUPBY_TASKS_QUEUE_BASE = "groupby_queue-"

class GroupbyTasksMiddleware(RabbitQueueMiddleware):
	def __init__(self, host = routing.RABBITMQ_HOST):
		super().__init__(GROUPBY_TASKS_QUEUE_BASE, host)


	def _callback_wrapper(self, callback):
		def real_callback(ch, method, properties, body):
			logging.info(f"action: groupby_rcv_msg | result: success | method: {method} | props: {properties} | body:{body}")
			
			# Wrap specific message
			wrapped_msg = GroupbyMessage(ch, method, properties.headers, body)

			return callback(wrapped_msg)
		return real_callback
