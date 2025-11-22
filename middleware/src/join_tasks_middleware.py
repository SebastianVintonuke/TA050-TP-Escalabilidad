from .rabbitmq_middleware import *
from .rabbitmq import constants as rbmq_consts

from .routing.csv_message import *


from .rabbitmq.blocking_manager import *
from .rabbitmq.blocking_batch_manager import *

JOIN_TASKS_QUEUE_BASE = "join_queue-{IND}"
JOIN_EXCHANGE = "join_exchange"
#JOIN_EXCHANGE = "direct"


class JoinTasksMiddleware(RabbitHashedExchangeMiddleware):

	def new_rabbit_manager(self, host):
		return BatchedRabbitMQManager(host)
		
		# return RabbitMQManager(host)


	def __init__(self, node_count,ind = 0, host = rbmq_consts.RABBITMQ_HOST):
		super().__init__(JOIN_TASKS_QUEUE_BASE, JOIN_EXCHANGE, node_count, ind , host =host)