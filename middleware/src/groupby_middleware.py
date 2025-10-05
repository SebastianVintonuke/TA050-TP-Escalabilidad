from .rabbitmq_middleware import *
from . import routing
from .routing.csv_message import *

GROUPBY_TASKS_QUEUE_BASE = "groupby_queue-{IND}"
GROUPBY_EXCHANGE = "groupby_exchange"
#GROUPBY_EXCHANGE = "direct"

class GroupbyTasksMiddleware(RabbitExchangeMiddleware):
	def __init__(self, node_count,ind = 0, host = routing.RABBITMQ_HOST):
		super().__init__(GROUPBY_TASKS_QUEUE_BASE.format(IND= ind), GROUPBY_EXCHANGE , host =host)
		self.node_count = int(node_count)

	# Redefine since it has custom exchange name. If it was just 'direct' or 'fanout' exchange name it would not be needed
	def _get_exchange_type(self): 
		return 'direct'

	def send(self, hashed_message_builder: CSVHashedMessageBuilder):
		target = GROUPBY_TASKS_QUEUE_BASE.format(IND= hashed_message_builder.hash_in(self.node_count))
		headers = hashed_message_builder.get_headers()
		payload = hashed_message_builder.serialize_payload()
		ind=0
		while self._send(target, headers,payload) and ind < SEND_RETRIES:
			ind+=1
			loggin.info(f"Retrying send {ind} of {headers}")

		if ind >= SEND_RETRIES:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at send, failed to many times")
