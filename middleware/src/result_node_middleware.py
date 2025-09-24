from .rabbitmq_middleware import *
from . import routing
from middleware.routing import result_message

RESULTS_QUEUE_BASE = "results_queue"

class ResultNodeMiddleware(RabbitQueueMiddleware):
	def __init__(self, host = routing.RABBITMQ_HOST, queue_name = RESULTS_QUEUE_BASE):
		super().__init__(queue_name, host)

	def _callback_wrapper(self, callback):
		def real_callback(ch, method, properties, body):
			logging.info(f"action: result_rcv_msg | result: success | method: {method} | props: {properties} | body:{body}")
			
			# Wrap specific message
			wrapped_msg = result_message.ResultMessage(ch, method, properties.headers, body)

			return callback(wrapped_msg)
		return real_callback
