from ..errors import *
from . import utils
import logging
import threading
from ..routing.csv_message import CSVMessageBuilder, CSVMessage
from ..routing.header_fields import BaseHeaders
from ..routing.batch_ack_actions import BatchingACKManager

from .blocking_manager import RabbitMQManager,RabbitMQChannel

DEFAULT_EXCHANGE = ''
CONNECTIONS_ATTMPS = 10

class BatchedRabbitMQChannel(RabbitMQChannel):
	def __init__(self, channel):
		super().__init__(channel)

	# Wrapper for rbmq
	# this class does it by batches.

	def _callback_wrapper(self, callback):

		map_ack_groups = BatchingACKManager()
		def real_callback(ch, method, properties, body):
			headers = BaseHeaders.from_headers(properties.headers)
			try:
				# headers.tag = method.delivery_tag
				ack_info = callback(headers, body) # Handle msg

				if ack_info == None: # Retu == None == auto ack only this packet.
					ch.basic_ack(delivery_tag=method.delivery_tag)
					return

				# Ack info includes a ack_group for acks and the action i.e wether accumulate, ack or nack. == None means accumulate
				map_ack_groups.new_ack_info(method.delivery_tag, ch, *ack_info)

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