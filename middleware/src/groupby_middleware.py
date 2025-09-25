from .rabbitmq_middleware import *
from . import routing
from .routing.groupby_message import *

GROUPBY_TASKS_QUEUE_BASE = "groupby_queue-{IND}"
GROUPBY_EXCHANGE = "groupby_exchange"

class GroupbyTasksMiddleware(RabbitExchangeMiddleware):
	def __init__(self, groupby_node_count,ind = 1, host = routing.RABBITMQ_HOST):
		super().__init__(GROUPBY_TASKS_QUEUE_BASE.format(IND= ind), GROUPBY_EXCHANGE , host =host)
		self.groupby_node_count = groupby_node_count


	def _callback_wrapper(self, callback):
		def real_callback(ch, method, properties, body):
			logging.info(f"action: groupby_rcv_msg | result: success | method: {method} | props: {properties} | body:{body}")
			
			# Wrap specific message
			wrapped_msg = GroupbyMessage(ch, method, properties.headers, body)

			return callback(wrapped_msg)
		return real_callback

	def send(self, hashed_message_builder):
		try:
			target = GROUPBY_TASKS_QUEUE_BASE.format(IND= hashed_message_builder.hash_in(self.groupby_node_count))
			self._send(target, hashed_message_builder.get_headers(),hashed_message_builder.serialize_payload())
		except routing.RoutingConnectionErrors as e:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at send: {e}") from e
		except Exception as e:
			raise MessageMiddlewareMessageError(f"Message handling error at send: {e}") from e
	