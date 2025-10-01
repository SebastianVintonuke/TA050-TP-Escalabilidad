from .middleware import *
from .errors import *
from . import routing
import logging
from .routing.csv_message import CSVMessageBuilder, CSVMessage

DEFAULT_EXCHANGE = ''
class RabbitExchangeMiddleware(MessageMiddleware):
	def __init__(self, queue_name, exchange_type = DEFAULT_EXCHANGE, host = routing.RABBITMQ_HOST):
		self.queue_name = queue_name
		self.exch_name = exchange_type
		try:
			self._conn = routing.try_open_connection(host, 10) # Can fail but should we wrap the error on a MessageMiddlewareDisconnectedError?
			self.channel = self._conn.channel()
			self._declares()
		except Exception as e:
			raise MessageMiddlewareConnectError(f"RabbitMQ connect failed: {e}") from e

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
		self.channel.basic_publish(exchange=self.exch_name, routing_key=routing_key, body=serial_msg,  
			properties=routing.build_headers(headers))

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
			self.channel.basic_consume(
				queue=self.queue_name, on_message_callback=self._callback_wrapper(on_message_callback), auto_ack=False)
			self.channel.start_consuming()
		except routing.RoutingConnectionErrors as e:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at consume: {e}") from e
		except Exception as e:
			raise MessageMiddlewareMessageError(f"RabbitMQ message handling error: {e}") from e

	# Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
	# Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	# Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	def send(self, message_builder: CSVMessageBuilder):
		try:
			self._send(self.queue_name, message_builder.get_headers(),message_builder.serialize_payload())
		except routing.RoutingConnectionErrors as e:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at send: {e}") from e
		except Exception as e:
			raise MessageMiddlewareMessageError(f"Message handling error at send: {e}") from e
	
	# Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
	# no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
	# Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.

	def stop_consuming(self):
		try:
			self.channel.stop_consuming()
		except routing.RoutingConnectionErrors as e:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at stop consume: {e}") from e

	# Se desconecta de la cola o exchange al que estaba conectado.
	# Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
	def close(self):
		try:
			self.channel.stop_consuming()
			self._conn.close()
			#self.channel = None # We could... and throw errors when you try to do something when closed... but is it needed?
		except Exception as e:
			raise MessageMiddlewareCloseError(f"error: {e}") from e

	# Se fuerza la eliminación remota de la cola o exchange.
	# Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareDeleteError.
	def delete(self):
		try:
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
