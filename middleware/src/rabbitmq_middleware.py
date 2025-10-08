from .middleware import *
from .errors import *
from . import routing
import logging
from .routing.csv_message import CSVMessageBuilder, CSVMessage

DEFAULT_EXCHANGE = ''
CONNECTIONS_ATTMPS = 10
SEND_RETRIES = 3

"""
	try:
		self._start_connection()
	except Exception as e:
		raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at restart connection : {e}") from e
"""


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
				properties=routing.build_headers(headers))
		except routing.RoutingRestartError as e:
			logging.error(f"Routing connection error happened at send, restart connection? {e}")
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at send: {e}, restart?") from e
			#return True
		except routing.RoutingConnectionErrors as e:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at send: {e}") from e
		except Exception as e:
			raise MessageMiddlewareMessageError(f"Message handling error at send: {e}") from e		
		
		#return False



	# Wrapper for rbmq messages, subclasses might override this in case they want to have specific types of messages
	def _callback_wrapper(self, callback):
		def real_callback(ch, method, properties, body):
			#logging.info(f"action: msg_recv | result: success | queue: {self.queue_name} | method: {method} | props: {properties} | body:{body}")
			#CSVMessage(properties.headers, body)

			try:
				ack_msg = callback(properties.headers, body) # Handle msg

				if ack_msg:
					ch.basic_ack(delivery_tag = method.delivery_tag)
				else:
					# If its explicitly told not to ack then dont requeue, If requeue is desired, throw exception(for now?)
					ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False) 				

			except Exception as e:
				logging.error(f"Message handling failed {properties.headers} requeue again: {e}")
				ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)	


		return real_callback

	def declare_consumue(self, queue_name, on_message_callback):
		self.channel.basic_consume(
			queue=queue_name, on_message_callback=self._callback_wrapper(on_message_callback), auto_ack=False)


	def start_consuming(self, prefetch_count = 2):
		try:
			self.channel.basic_qos(prefetch_count=prefetch_count)
			self.channel.start_consuming()
		except routing.RoutingRestartError as e:
			logging.error(f"Routing connection error happened at consume, restart connection {e}")
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at consume, connection restart?: {e}") from e
		except routing.RoutingConnectionErrors as e:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at consume: {e}") from e
		except Exception as e:
			raise MessageMiddlewareMessageError(f"RabbitMQ message handling error: {e}") from e




class RabbitMQManager:
	def __init__(self, host):
		self.host = host
		self.channels = []
		routing.wait_middleware_init()
		self._start_connection()

	def _start_connection(self):
		self._conn = routing.try_open_connection(self.host, CONNECTIONS_ATTMPS) # Can fail but should we wrap the error on a MessageMiddlewareDisconnectedError?

	def open_channel(self):
		channel = RabbitMQChannel(self._conn.channel())
		self.channels.append(channel)
		return channel


	def stop_consuming(self):
		for channel in self.channels:
			if channel.is_open:
				channel.stop_consuming()

	def _stop_action(self, stop_signal):
		self.stop_consuming()
		stop_signal.set()				

	def async_stop_consuming(self):
		stop_signal= threading.Event()

		# Schedule to trigger stop thread safely as soon as possible on connection.
		self._conn.add_callback_threadsafe(lambda: self._stop_action(stop_signal))

		stop_signal.wait()

	def close(self):
		for channel in self.channels:
			if channel.is_open:
				channel.close()

		if self._conn.is_open:
			self._conn.close()




class BaseRabbitMiddleware(MessageMiddleware):
	def __init__(self, host):
		try:
			self._rabbit_manager = RabbitMQManager(host)
			self._channel = self._rabbit_manager.open_channel()
		except Exception as e:
			raise MessageMiddlewareConnectError(f"RabbitMQ connect failed: {e}") from e

	def stop_consuming(self):
		try:
			self._rabbit_manager.stop_consuming()
		except routing.RoutingConnectionErrors as e:
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
		except routing.RoutingConnectionErrors as e:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at delete queue: {e}") from e
		except Exception as e:
			raise MessageMiddlewareDeleteError(str(e)) from e

class RabbitQueueMiddleware(BaseRabbitMiddleware):
	def __init__(self, queue_name, host = routing.RABBITMQ_HOST):
		super().__init__(host)
		self.queue_name = queue_name
		self._channel.declare_queues(queue_name)

	def start_consuming(self, on_message_callback):
		self._channel.declare_consumue(self.queue_name, on_message_callback)
		self._channel.start_consuming()

	def send(self, message_builder: CSVMessageBuilder):
		payload = message_builder.serialize_payload()
		headers = message_builder.get_headers()
		self._channel.send(self.queue_name, headers,payload)

	def _delete(self):
		if self.channel and self.channel.is_open:
			self.channel.queue_delete(self.queue_name)


class RabbitExchangeMiddleware(RabbitQueueMiddleware):
	def __init__(self, queue_name, exchange_name = DEFAULT_EXCHANGE, host = routing.RABBITMQ_HOST):
		super().__init__(queue_name, host)

		self.exch_name = exchange_name
		self._channel.exchange_declare(self.exch_name, self._get_exchange_type())
	def _get_exchange_type(self):
		return self.exch_name

	def start_consuming(self, on_message_callback):
		try:
			self._channel.bind_queue(self.queue_name, self.queue_name)
			self._channel.declare_consumue(self.queue_name, on_message_callback)
		except routing.RoutingConnectionErrors as e:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ queue binding and consume declaring failed because of connection: {e}") from e
		except Exception as e:
			raise MessageMiddlewareMessageError(f"RabbitMQ queue binding and consume declaring failed: {e}") from e

		self._channel.start_consuming()



class RabbitHashedExchangeMiddleware(RabbitExchangeMiddleware):
	def __init__(self, queue_name_base, exchange_name, node_count, ind = 0, host = routing.RABBITMQ_HOST):
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
	def __init__(self, route_keys, exchange_name = DEFAULT_EXCHANGE, host = routing.RABBITMQ_HOST):
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
		except routing.RoutingConnectionErrors as e:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ queue binding and consume declaring failed because of connection: {e}") from e
		except Exception as e:
			raise MessageMiddlewareMessageError(f"RabbitMQ queue binding and consume declaring failed: {e}") from e

		self._channel.start_consuming()


	def send(self, message_builder: CSVMessageBuilder):
		payload = message_builder.serialize_payload()
		headers = message_builder.get_headers()
		route_key = self.route_types.get(message_builder.types[0], message_builder.types[0]) 
		
		self._channel.send(route_key, headers,payload)

	def _delete(self):
		if self.channel and self.channel.is_open:
			for key in self.route_types.keys():
				self.channel.queue_delete(self.parse_queue_name(key))
