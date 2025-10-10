from .rabbitmq_middleware import *
from .rabbitmq import constants as rbmq_consts

from .routing.csv_message import *

SELECT_TASKS_QUEUE_BASE = "select_tasks_queue"

class SelectTasksMiddleware(RabbitQueueMiddleware):
	def __init__(self, host = rbmq_consts.RABBITMQ_HOST, queue_name = SELECT_TASKS_QUEUE_BASE):
		super().__init__(queue_name, host)
