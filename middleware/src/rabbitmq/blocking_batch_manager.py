from ..errors import *
from . import utils
import logging
import threading
from ..routing.csv_message import CSVMessageBuilder, CSVMessage
from ..routing.header_fields import BaseHeaders

from .blocking_manager import RabbitMQManager,RabbitMQChannel

DEFAULT_EXCHANGE = ''
CONNECTIONS_ATTMPS = 10

class BatchedRabbitMQChannel(RabbitMQChannel):
	def __init__(self, channel):
		super().__init__(channel)

	# Wrapper for rbmq
	# this class does it by batches.

	def _callback_wrapper(self, callback):

		msgs_to_ack = []
		def real_callback(ch, method, properties, body):
			headers = BaseHeaders.from_headers(properties.headers)
			try:
				# headers.tag = method.delivery_tag
				accum_to_batch = callback(headers, body) # Handle msg

				msgs_to_ack.append(method.delivery_tag)

				if accum_to_batch == False: # If told not to accumulate then send nacks.
					
					for tag in msgs_to_ack:						
						# If msg failed, requeue is desired else throw exception(for now?)
						ch.basic_nack(delivery_tag=tag, requeue=True)
					msgs_to_ack.clear()

				elif accum_to_batch == None: # No return functions do by default auto ack messages.. return True to avoid auto acking i.e accumulate in a batch
					
					for tag in msgs_to_ack:						
						# If msg failed, requeue is desired else throw exception(for now?)
						ch.basic_nack(delivery_tag=tag, requeue=True)
					msgs_to_ack.clear()


			except Exception as e:
				logging.error(f"Message handling failed {headers}")

				logging.error(f"msg method: {method} prop: {properties}")
				logging.error(f"payload: {body[:min(50,len(body))]} error: {e}")
				ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)	


		return real_callback


class BatchedRabbitMQManager(RabbitMQManager):
	def __init__(self, host):
		super().__init__(host)

	def open_channel(self):
		channel = BatchedRabbitMQChannel(self._conn.channel())
		self.channels.append(channel)
		return channel