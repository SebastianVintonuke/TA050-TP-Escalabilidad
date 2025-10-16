from .rabbitmq_middleware import *
from .rabbitmq import constants as rbmq_consts

from .routing.csv_message import *
from .routing.query_types import *

SELECT_TASKS_EXCHANGE = "select_tasks"
SELECT_ALL_TYPES = [QUERY_1,QUERY_2,QUERY_3,QUERY_4, ALL_FOR_TRANSACTIONS, ALL_FOR_TRANSACTIONS_ITEMS]
SELECT_TASKS_QUEUE_BASE= "select_tasks_queue"

class SelectTasksMultiMiddleware(RabbitMultiQueueExchangeMiddleware):
	def parse_queue_name_select(msg_type):
		return f"select-tasks-{msg_type}"		

	def __init__(self, queue_names = None, host = rbmq_consts.RABBITMQ_HOST):
		super().__init__(queue_names if queue_names else SELECT_ALL_TYPES, SELECT_TASKS_EXCHANGE, host)

	def parse_real_route_key(self, msg_type):
		return f"select-tasks-{msg_type}"		

	def parse_queue_name(self, msg_type):
		return SelectTasksMultiMiddleware.parse_queue_name_select(msg_type)		


class SelectTasksMiddleware(RabbitQueueMiddleware):
	def __init__(self, host = rbmq_consts.RABBITMQ_HOST, queue_name = SELECT_TASKS_QUEUE_BASE):
		super().__init__(queue_name, host)