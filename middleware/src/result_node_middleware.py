from .rabbitmq_middleware import *
from . import routing

RESULTS_QUEUE_BASE = "results_queue"

class ResultNodeMiddleware(RabbitQueueMiddleware):
	def __init__(self, host = routing.RABBITMQ_HOST, queue_name = RESULTS_QUEUE_BASE):
		super().__init__(queue_name, host)
