from .rabbitmq_middleware import *
from .rabbitmq import constants as rbmq_consts

from .routing.csv_message import *

JOIN_TASKS_QUEUE_BASE = "join_queue-{IND}"
JOIN_EXCHANGE = "join_exchange"
#JOIN_EXCHANGE = "direct"


class JoinTasksMiddleware(RabbitHashedExchangeMiddleware):
	def __init__(self, node_count,ind = 0, host = rbmq_consts.RABBITMQ_HOST):
		super().__init__(JOIN_TASKS_QUEUE_BASE, JOIN_EXCHANGE, node_count, ind , host =host)