from ..errors import *
from . import utils
import logging
import threading
from ..routing.csv_message import CSVMessageBuilder, CSVMessage
from ..routing.header_fields import BaseHeaders


DEFAULT_EXCHANGE = ''
CONNECTIONS_ATTMPS = 10

class RabbitMQChannel:
	def __init__(self, channel):
		self.channel = channel
		self.exch_name= DEFAULT_EXCHANGE

	def exchange_declare(self, exch_name, exch_type):
		self.channel.exchange_declare(
		    exchange=exch_name,
		    exchange_type=exch_type,
		    #durable=True 
		)

		self.exch_name = exch_name


	def declare_queues(self, *names):
		for name in names:
			self.channel.queue_declare(queue= name)

	def bind_queue(self, name, routing_key):
		self.channel.queue_bind(queue=name, exchange=self.exch_name, routing_key=routing_key)


	# Serial basic _send
	def send(self, routing_key, headers, serial_msg):
		try:
			self.channel.basic_publish(exchange=self.exch_name, routing_key=routing_key, body=serial_msg,  
				properties=utils.build_headers(headers))
		except utils.RoutingRestartError as e:
			logging.error(f"Routing connection error happened at send, restart connection? {e}")
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at send: {e}, restart?") from e
			#return True
		except utils.RoutingConnectionErrors as e:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at send: {e}") from e
		except Exception as e:
			raise MessageMiddlewareMessageError(f"Message handling error at send: {e}") from e		
		
		#return False



	# Wrapper for rbmq messages, subclasses might override this in case they want to have specific types of messages
	def _callback_wrapper(self, callback):
		def real_callback(ch, method, properties, body):
			#logging.info(f"action: msg_recv | result: success | queue: {self.queue_name} | method: {method} | props: {properties} | body:{body}")
			#CSVMessage(properties.headers, body)
			headers = BaseHeaders.from_headers(properties.headers)
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

	def declare_consume(self, queue_name, on_message_callback):
		self.channel.basic_consume(
			queue=queue_name, on_message_callback=self._callback_wrapper(on_message_callback), auto_ack=False)

	def declare_raw_consume(self, queue_name, on_message_callback):
		self.channel.basic_consume(
			queue=queue_name, on_message_callback=on_message_callback, auto_ack=False)

	def start_consume(self, prefetch_count = 2):
		try:
			self.channel.basic_qos(prefetch_count=prefetch_count)
			self.channel.start_consuming()
		except utils.RoutingRestartError as e:
			logging.error(f"Routing connection error happened at consume, restart connection {e}")
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at consume, connection restart?: {e}") from e
		except utils.RoutingConnectionErrors as e:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at consume: {e}") from e
		except Exception as e:
			raise MessageMiddlewareMessageError(f"RabbitMQ message handling error: {e}") from e

	def set_prefetch(self, prefetch_count = 2):
		self.channel.basic_qos(prefetch_count=prefetch_count)


class RabbitMQManager:
	def __init__(self, host):
		self.host = host
		self.channels = []
		self.is_consuming = threading.Event()
		self.is_stopped = threading.Event()

		utils.wait_middleware_init()
		self._start_connection()


	def _start_connection(self):
		self._conn = utils.try_open_connection(self.host, CONNECTIONS_ATTMPS) # Can fail but should we wrap the error on a MessageMiddlewareDisconnectedError?

	def open_channel(self):
		channel = RabbitMQChannel(self._conn.channel())
		self.channels.append(channel)
		return channel

	def stop_channels(self):
		for channel in self.channels:
			if channel.is_open:
				channel.stop_consuming()

	def stop_consuming(self):
		self.is_consuming.clear()
		#self.stop_channels()

	def async_stop_consuming(self):
		# Schedule to trigger stop thread safely as soon as possible on connection.
		self._conn.add_callback_threadsafe(self.stop_consuming)
		self.is_stopped.wait()

	def close(self):
		for channel in self.channels:
			if channel.is_open:
				channel.close()

		if self._conn.is_open:
			self._conn.close()

	def _reset_state(self):
		self.is_stopped.set()
		self.is_consuming.clear()

	def wait_stopped(self):
		self.is_stopped.wait()

	def start_consuming(self, time_limit= 1):
		try:
			self.is_stopped.clear()
			self.is_consuming.set()

			# Manually trigger process data events, this allows for tight control on when to stop and
			# ... it allows for starting to consume on multiple channels
			while self.is_consuming.is_set():
				self.connection.process_data_events(time_limit=time_limit)
			self._reset_state()
		except utils.RoutingRestartError as e:
			logging.error(f"Routing connection error happened at consume, restart connection {e}")
			self._reset_state()
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at consume, connection restart?: {e}") from e
		except utils.RoutingConnectionErrors as e:
			self._reset_state()
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at consume: {e}") from e
		except Exception as e:
			self._reset_state()
			raise MessageMiddlewareMessageError(f"RabbitMQ message handling error: {e}") from e			

