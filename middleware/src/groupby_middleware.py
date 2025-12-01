from .rabbitmq_middleware import *
from .rabbitmq import constants as rbmq_consts
from .routing.csv_message import *

from .rabbitmq.blocking_manager import *
from .rabbitmq.blocking_batch_manager import *

# BatchedRabbitMQManager

GROUPBY_TASKS_QUEUE_BASE = "groupby_queue-{IND}-{TYPE}"
GROUPBY_EXCHANGE = "groupby_exchange"


# RabbitHashedExchangeMiddleware
class GroupbyTasksMiddleware(RabbitExchangeMiddleware):
	def new_rabbit_manager(self, host):
		return BatchedRabbitMQManager(host)
		
		# return RabbitMQManager(host)

	# def __init__(self, node_count,ind = 0, host = rbmq_consts.RABBITMQ_HOST):
	# 	super().__init__(GROUPBY_TASKS_QUEUE_BASE, GROUPBY_EXCHANGE, node_count, ind , host =host)


	def __init__(self, node_count, ind = 0, type = "", host = rbmq_utils.RABBITMQ_HOST):
		super().__init__(GROUPBY_TASKS_QUEUE_BASE.format(IND= ind, TYPE = type), GROUPBY_EXCHANGE , host =host)
		self.queue_name_base = GROUPBY_TASKS_QUEUE_BASE
		self.node_count = int(node_count)

	def send(self, hashed_message_builder): #: 

		target = hashed_message_builder.hash_in(self.queue_name_base, self.node_count)
		
		headers = hashed_message_builder.get_headers()
		payload = hashed_message_builder.serialize_payload()

		self._channel.send(target, headers,payload)

