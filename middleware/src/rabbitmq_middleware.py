from .middleware import *
from .errors import *
from . import routing
import logging
from .routing.csv_message import CSVMessageBuilder, CSVMessage

DEFAULT_EXCHANGE = ''
CONNECTIONS_ATTMPS = 10
SEND_RETRIES = 3

class RabbitExchangeMiddleware(MessageMiddleware):
	def __init__(self, queue_name, exchange_type = DEFAULT_EXCHANGE, host = routing.RABBITMQ_HOST):
		self.queue_name = queue_name
		self.exch_name = exchange_type
		self.host = host
		try:
			routing.wait_middleware_init()			
			self._start_connection()
		except Exception as e:
			raise MessageMiddlewareConnectError(f"RabbitMQ connect failed: {e}") from e

	def _start_connection(self):
		self._conn = routing.try_open_connection(self.host, CONNECTIONS_ATTMPS) # Can fail but should we wrap the error on a MessageMiddlewareDisconnectedError?
		self.channel = self._conn.channel()
		self._declares()

	def _get_routing_key(self):
		return self.queue_name

	def _get_exchange_type(self):
		return self.exch_name

	def _declares(self):
		self.channel.queue_declare(queue=self.queue_name)
		
		self.channel.exchange_declare(
		    exchange=self.exch_name,
		    exchange_type=self._get_exchange_type(),
		    #durable=True 
		)

	def _init_bind(self):
		self.channel.queue_bind(queue=self.queue_name, exchange=self.exch_name, routing_key=self._get_routing_key())


	# Serial basic _send
	def _send(self, routing_key, headers, serial_msg):
		try:
			self.channel.basic_publish(exchange=self.exch_name, routing_key=routing_key, body=serial_msg,  
				properties=routing.build_headers(headers))
		except routing.RoutingRestartError as e:
			logging.error(f"Routing connection error happened at send, restart connection {e}")
			try:
				self._start_connection()
			except Exception as e:
				raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at restart connection : {e}") from e
			return True
		except routing.RoutingConnectionErrors as e:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at send: {e}") from e
		except Exception as e:
			raise MessageMiddlewareMessageError(f"Message handling error at send: {e}") from e		
		
		return False

	# Wrapper for rbmq messages, subclasses might override this in case they want to have specific types of messages
	def _callback_wrapper(self, callback):
		def real_callback(ch, method, properties, body):
			#logging.info(f"action: msg_recv | result: success | queue: {self.queue_name} | method: {method} | props: {properties} | body:{body}")
			
			callback(CSVMessage(properties.headers, body)) # Handle msg
			
			# If reached here .. all ok, then ack msg
			ch.basic_ack(delivery_tag = method.delivery_tag)


		return real_callback

	# Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
	# cada mensaje de datos o de control.
	# Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	# Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	def start_consuming(self, on_message_callback):
		try:
			self._init_bind()

			self.channel.basic_qos(prefetch_count=4)

			self.channel.basic_consume(
				queue=self.queue_name, on_message_callback=self._callback_wrapper(on_message_callback), auto_ack=False)
			self.channel.start_consuming()
		except routing.RoutingRestartError as e:
			logging.error(f"Routing connection error happened at consume, restart connection {e}")
			try:
				self._start_connection()
			except Exception as e:
				raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at restart connection : {e}") from e

			raise MessageMiddlewareMessageError(f"RabbitMQ connection restart: {e}") from e
		
		except routing.RoutingConnectionErrors as e:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at consume: {e}") from e
		except Exception as e:
			raise MessageMiddlewareMessageError(f"RabbitMQ message handling error: {e}") from e

	# Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
	# Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	# Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	def send(self, message_builder: CSVMessageBuilder):
		headers = message_builder.get_headers()
		payload = message_builder.serialize_payload()
		ind=0
		while self._send(self.queue_name, headers,payload) and ind < SEND_RETRIES:
			ind+=1
			logging.info(f"Retrying send {ind} of {headers}")

		if ind >= SEND_RETRIES:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at send, failed to many times")

	
	# Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
	# no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
	# Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.

	def stop_consuming(self):
		try:
			if self.channel.is_open:
				self.channel.stop_consuming()
				# Allow time to process CancelOk or remaining events
				self._conn.process_data_events(time_limit=0.5)
		except routing.RoutingConnectionErrors as e:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at stop consume: {e}") from e

	# Se desconecta de la cola o exchange al que estaba conectado.
	# Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
	def close(self):
		try:
			self.stop_consuming()
			if self.channel and self.channel.is_open:
				self.channel.close()
			if self._conn and self._conn.is_open:
				self._conn.close()
			
			#self.channel = None # We could... and throw errors when you try to do something when closed... but is it needed?
		except Exception as e:
			raise MessageMiddlewareCloseError(f"error: {e}") from e

	# Se fuerza la eliminación remota de la cola o exchange.
	# Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareDeleteError.
	def delete(self):
		try:
			if self.channel and self.channel.is_open:
				self.channel.queue_delete(self.queue_name)
		except routing.RoutingConnectionErrors as e:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at delete queue: {e}") from e
		except Exception as e:
			raise MessageMiddlewareDeleteError(str(e)) from e


# Not used at the moment.
class RabbitQueueMiddleware(RabbitExchangeMiddleware):
	def __init__(self, queue_name, host = routing.RABBITMQ_HOST):
		super().__init__(queue_name, DEFAULT_EXCHANGE, host)

	def _init_bind(self):
		pass

	def _declares(self):
		self.channel.queue_declare(queue=self.queue_name)
