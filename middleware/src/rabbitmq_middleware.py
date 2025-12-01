from .middleware import *
from .errors import *
import logging
from .routing.csv_message import CSVMessageBuilder, CSVMessage
from .routing.header_fields import BaseHeaders
from .rabbitmq import utils as rbmq_utils
from .rabbitmq.blocking_manager import *

class BaseRabbitMiddleware(MessageMiddleware):

	def new_rabbit_manager(self, host):
		return RabbitMQManager(host)

	def __init__(self, host):
		try:
			self._rabbit_manager = self.new_rabbit_manager(host)
			self._channel = self._rabbit_manager.open_channel()
		except Exception as e:
			raise MessageMiddlewareConnectError(f"RabbitMQ connect failed: {e}") from e


	def ack_message(self, delivery_tag):
		self._channel.ack_message(delivery_tag)
		
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
		self._channel.declare_consume(self.queue_name, on_message_callback)
		self._channel.start_consume()

	def send(self, message_builder: CSVMessageBuilder):
		payload = message_builder.serialize_payload()
		headers = message_builder.get_headers()
		self._channel.send(self.queue_name, headers,payload)

	def _delete(self):
		if self.channel and self.channel.is_open:
			self.channel.queue_delete(self.queue_name)


class RabbitExchangeMiddleware(BaseRabbitMiddleware):
	def __init__(self, queue_name, exchange_name = DEFAULT_EXCHANGE, host = rbmq_utils.RABBITMQ_HOST):
		super().__init__(host)
		self.queue_name = queue_name
		self._channel.declare_queues(queue_name)
		
		self.exch_name = exchange_name
		self._channel.exchange_declare(self.exch_name, self._get_exchange_type())
	def _get_exchange_type(self):
		return "direct"

	def start_consuming(self, on_message_callback):
		try:
			self._channel.bind_queue(self.queue_name, self.queue_name)
			self._channel.declare_consume(self.queue_name, on_message_callback)
		except rbmq_utils.RoutingConnectionErrors as e:

			raise MessageMiddlewareDisconnectedError(f"RabbitMQ queue binding and consume declaring failed because of connection: {e}") from e
		except Exception as e:
			raise MessageMiddlewareMessageError(f"RabbitMQ queue binding and consume declaring failed: {e}") from e

		self._channel.start_consume()

	def send(self, message_builder: CSVMessageBuilder):
		payload = message_builder.serialize_payload()
		headers = message_builder.get_headers()
		self._channel.send(self.queue_name, headers,payload)

	def _delete(self):
		if self.channel and self.channel.is_open:
			self.channel.queue_delete(self.queue_name)


class RabbitExchangeMiddlewareTypeNamed(RabbitExchangeMiddleware):
	def __init__(self, queue_name, exchange_name = DEFAULT_EXCHANGE, host = rbmq_utils.RABBITMQ_HOST):
		self.exch_type = exchange_name
		super().__init__(queue_name, exchange_name, host)

	def _get_exchange_type(self):
		return self.exch_type



class RabbitHashedExchangeMiddleware(RabbitExchangeMiddleware):
	def __init__(self, queue_name_base, exchange_name, node_count, ind = 0, host = rbmq_utils.RABBITMQ_HOST):
		super().__init__(queue_name_base.format(IND= ind), exchange_name , host =host)
		self.queue_name_base = queue_name_base
		self.node_count = int(node_count)

	def send(self, hashed_message_builder): #: 

		target = hashed_message_builder.hash_in(self.queue_name_base, self.node_count)
		
		headers = hashed_message_builder.get_headers()
		payload = hashed_message_builder.serialize_payload()

		self._channel.send(target, headers,payload)

