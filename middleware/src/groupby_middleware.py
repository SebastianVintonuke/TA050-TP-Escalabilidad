from .rabbitmq_middleware import *
from .rabbitmq import constants as rbmq_consts
from .routing.csv_message import *

GROUPBY_TASKS_QUEUE_BASE = "groupby_queue-{IND}"
GROUPBY_EXCHANGE = "groupby_exchange"

class GroupbyTasksMiddleware(RabbitHashedExchangeMiddleware):
	def __init__(self, node_count,ind = 0, host = rbmq_consts.RABBITMQ_HOST):
		super().__init__(GROUPBY_TASKS_QUEUE_BASE, GROUPBY_EXCHANGE, node_count, ind , host =host)
