from .middleware import *
from .errors import *
import logging
from .routing.csv_message import CSVMessageBuilder, CSVMessage
from .routing.header_fields import BaseHeaders

from .rabbitmq import utils as rbmq_utils
from .rabbitmq.blocking_manager import *

class BaseRabbitMiddleware(MessageMiddleware):
	def __init__(self, host):
		try:
			self._rabbit_manager = RabbitMQManager(host)
			self._channel = self._rabbit_manager.open_channel()
		except Exception as e:
			raise MessageMiddlewareConnectError(f"RabbitMQ connect failed: {e}") from e

	def stop_consuming(self):
		try:
			self._rabbit_manager.stop_channels() # Asumming not async
			#self._rabbit_manager.stop_consuming()
		except rbmq_utils.RoutingConnectionErrors as e:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at stop consume: {e}") from e

	def close(self):
		try:
			self._rabbit_manager.stop_consuming()
			self._rabbit_manager.close()
		except Exception as e:
			raise MessageMiddlewareCloseError(f"error: {e}") from e

	def _delete(self):
		pass

	def delete(self):
		try:
			self._delete()
		except rbmq_utils.RoutingConnectionErrors as e:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at delete queue: {e}") from e
		except Exception as e:
			raise MessageMiddlewareDeleteError(str(e)) from e

class RabbitQueueMiddleware(BaseRabbitMiddleware):
	def __init__(self, queue_name, host = rbmq_utils.RABBITMQ_HOST):
		super().__init__(host)
		self.queue_name = queue_name
		self._channel.declare_queues(queue_name)

	def start_consuming(self, on_message_callback):
		self._channel.declare_consumue(self.queue_name, on_message_callback)
		self._channel.start_consume()

	def send(self, message_builder: CSVMessageBuilder):
		payload = message_builder.serialize_payload()
		headers = message_builder.get_headers()
		self._channel.send(self.queue_name, headers,payload)

	def _delete(self):
		if self.channel and self.channel.is_open:
			self.channel.queue_delete(self.queue_name)


class RabbitExchangeMiddleware(RabbitQueueMiddleware):
	def __init__(self, queue_name, exchange_name = DEFAULT_EXCHANGE, host = rbmq_utils.RABBITMQ_HOST):
		super().__init__(queue_name, host)

		self.exch_name = exchange_name
		self._channel.exchange_declare(self.exch_name, self._get_exchange_type())
	def _get_exchange_type(self):
		return self.exch_name

	def start_consuming(self, on_message_callback):
		try:
			self._channel.bind_queue(self.queue_name, self.queue_name)
			self._channel.declare_consumue(self.queue_name, on_message_callback)
		except rbmq_utils.RoutingConnectionErrors as e:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ queue binding and consume declaring failed because of connection: {e}") from e
		except Exception as e:
			raise MessageMiddlewareMessageError(f"RabbitMQ queue binding and consume declaring failed: {e}") from e

		self._channel.start_consume()



class RabbitHashedExchangeMiddleware(RabbitExchangeMiddleware):
	def __init__(self, queue_name_base, exchange_name, node_count, ind = 0, host = rbmq_utils.RABBITMQ_HOST):
		super().__init__(queue_name_base.format(IND= ind), exchange_name , host =host)
		self.queue_name_base = queue_name_base
		self.node_count = int(node_count)

	# Redefine since it has custom exchange name. If it was just 'direct' or 'fanout' exchange name it would not be needed
	def _get_exchange_type(self): 
		return 'direct'


	def send(self, hashed_message_builder): #: 
		target = self.queue_name_base.format(IND= hashed_message_builder.hash_in(self.node_count))
		headers = hashed_message_builder.get_headers()
		payload = hashed_message_builder.serialize_payload()

		self._channel.send(target, headers,payload)






class RabbitMultiQueueExchangeMiddleware(BaseRabbitMiddleware):
	def __init__(self, route_keys, exchange_name = DEFAULT_EXCHANGE, host = rbmq_utils.RABBITMQ_HOST):
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
		return self.exch_name

	def parse_queue_name(self, queue_type):
		return self.exch_name+"-"+queue_type

	def parse_real_route_key(self, msg_type):
		return f"tasks-{msg_type}"		
	

	def start_consuming(self, on_message_callbacks):
		try:
			for r_type, r_key in self.route_types.items():
				queue_name= self.parse_queue_name(r_type)
				self._channel.bind_queue(queue_name, r_key)

				callback = on_message_callbacks.get(r_type, None)
				if callback:
					self._channel.declare_consumue(queue_name, callback)
		except rbmq_utils.RoutingConnectionErrors as e:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ queue binding and consume declaring failed because of connection: {e}") from e
		except Exception as e:
			raise MessageMiddlewareMessageError(f"RabbitMQ queue binding and consume declaring failed: {e}") from e

		self._channel.start_consume()


	def send(self, message_builder: CSVMessageBuilder):
		payload = message_builder.serialize_payload()
		headers = message_builder.get_headers()
		route_key = self.route_types.get(message_builder.types[0], message_builder.types[0]) 
		
		self._channel.send(route_key, headers,payload)

	def _delete(self):
		if self.channel and self.channel.is_open:
			for key in self.route_types.keys():
				self.channel.queue_delete(self.parse_queue_name(key))
