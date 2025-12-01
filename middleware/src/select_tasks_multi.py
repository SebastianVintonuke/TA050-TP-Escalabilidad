
from .rabbitmq_middleware import *
from .rabbitmq import constants as rbmq_consts

from .routing.csv_message import *
from .routing.query_types import *


class RabbitMultiQueueExchangeMiddleware(BaseRabbitMiddleware):
	def __init__(self, route_keys, exchange_name, host = rbmq_utils.RABBITMQ_HOST):

		super().__init__(host)
		self.route_types = {}
		self.exch_name = exchange_name

		for route_type in route_keys:
			self.route_types[route_type] = self.parse_real_route_key(route_type)
		
		self._initialize()

	def _initialize(self):
		queue_names = tuple(self.parse_queue_name(key) for key in self.route_types.keys())
		self._channel.declare_queues(*queue_names)

		# Exchange declare
		self._channel.exchange_declare(self.exch_name, self._get_exchange_type())

	def _get_exchange_type(self):
		return "direct"

	def parse_queue_name(self, queue_type):
		return self.exch_name+"-"+queue_type

	def parse_real_route_key(self, msg_type):
		return f"tasks-{msg_type}"		
	





	def _callback_wrapper(self, callback, r_type):
		def real_callback(ch, method, properties, body):
			#logging.info(f"action: msg_recv | result: success | queue: {self.queue_name} | method: {method} | props: {properties} | body:{body}")
			#CSVMessage(properties.headers, body)
			headers = BaseHeaders.from_headers(properties.headers)

			headers.types = [r_type]
			try:
				msg_failed = callback(headers, body) # Handle msg

				if msg_failed:
					# If msg failed, requeue is desired else throw exception(for now?)
					ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True) 				
				else:
					ch.basic_ack(delivery_tag = method.delivery_tag)

			except Exception as e:
				logging.error(f"Message handling failed {headers}")
				logging.error(f"payload: {body[:min(50,len(body))]} error: {e}")
				ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)	


		return real_callback


	def start_consuming(self, on_message_callbacks):
		try:
			for r_type, r_key in self.route_types.items():
				queue_name= self.parse_queue_name(r_type)
				self._channel.bind_queue(queue_name, r_key)

				callback = on_message_callbacks.get(r_type, None)
				if callback:
					self._channel.declare_raw_consume(queue_name, self._callback_wrapper(callback, r_type))
		except rbmq_utils.RoutingConnectionErrors as e:

			raise MessageMiddlewareDisconnectedError(f"RabbitMQ queue binding and consume declaring failed because of connection: {e}") from e
		except Exception as e:
			raise MessageMiddlewareMessageError(f"RabbitMQ queue binding and consume declaring failed: {e}") from e

		self._channel.start_consume()


	def send(self, message_builder: CSVMessageBuilder):
		payload = message_builder.serialize_payload()
		
		for single_headers in message_builder.headers.split():

			route_key = self.route_types.get(single_headers.types[0], single_headers.types[0])
			logging.info(f"----> sending to {self._channel.exch_name}/ {route_key} len: {len(payload)} {single_headers.to_dict_no_type()}")
			self._channel.send(route_key, single_headers.to_dict_no_type(),payload)

	def _delete(self):
		if self.channel and self.channel.is_open:
			for key in self.route_types.keys():
				self.channel.queue_delete(self.parse_queue_name(key))


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
