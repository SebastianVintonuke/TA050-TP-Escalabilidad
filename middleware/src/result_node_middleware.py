from .rabbitmq_middleware import *
from .rabbitmq import constants as rbmq_consts

RESULTS_QUEUE_BASE = "results_queue"

class ResultNodeMiddleware(RabbitQueueMiddleware):
	def __init__(self, host = rbmq_consts.RABBITMQ_HOST, queue_name = RESULTS_QUEUE_BASE):
		super().__init__(queue_name, host)
