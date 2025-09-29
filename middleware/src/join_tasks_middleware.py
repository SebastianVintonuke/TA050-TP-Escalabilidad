from .rabbitmq_middleware import *
from . import routing
from .routing.csv_message import *

JOIN_TASKS_QUEUE_BASE = "join_queue-{IND}"
JOIN_EXCHANGE = "join_exchange"
#JOIN_EXCHANGE = "direct"

class JoinTasksMiddleware(RabbitExchangeMiddleware):
	def __init__(self, groupby_node_count,ind = 0, host = routing.RABBITMQ_HOST):
		super().__init__(JOIN_TASKS_QUEUE_BASE.format(IND= ind), JOIN_EXCHANGE , host =host)
		self.groupby_node_count = int(groupby_node_count)

	# Redefine since it has custom exchange name. If it was just 'direct' or 'fanout' exchange name it would not be needed
	def _get_exchange_type(self): 
		return 'direct'

	def send(self, hashed_message_builder: CSVHashedMessageBuilder):
		try:
			target = JOIN_TASKS_QUEUE_BASE.format(IND= hashed_message_builder.hash_in(self.groupby_node_count))
			self._send(target, hashed_message_builder.get_headers(),hashed_message_builder.serialize_payload())
		except routing.RoutingConnectionErrors as e:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at send: {e}") from e
		except Exception as e:
			raise MessageMiddlewareMessageError(f"Message handling error at send: {e}") from e
	