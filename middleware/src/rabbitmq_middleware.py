from .middleware import *
from .errors import *
from . import routing
import logging

class RabbitQueueMiddleware(MessageMiddlewareQueue):
	def __init__(self, queue_name, host = routing.RABBITMQ_HOST):
		self.queue_name = queue_name
		self.exch_type = ''
		try:
			self._conn = routing.try_open_connection(host, 10) # Can fail but should we wrap the error on a MessageMiddlewareDisconnectedError?
			self.channel = self._conn.channel()
			self.channel.queue_declare(queue=self.queue_name)

		except Exception as e:
			raise MessageMiddlewareConnectError(f"RabbitMQ connect failed: {e}") from e


	# Serial basic _send
	def _send(self, headers, serial_msg):
		self.channel.basic_publish(exchange=self.exch_type, routing_key=self.queue_name, body=serial_msg,  
			properties=pika.BasicProperties(
				headers=headers
			))

	# Wrapper for rbmq messages, subclasses might override this in case they want to have specific types of messages
	def _callback_wrapper(self, callback):
		def real_callback(ch, method, properties, body):
			logging.info(f"action: rcv_msg | result: success | method: {method} | props: {properties} | body:{body}")
			
			wrapped_msg = routing.ChannelMessage(ch, method, properties.headers, body)

			return callback(wrapped_msg)
		return real_callback

	# Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
	# cada mensaje de datos o de control.
	# Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	# Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	def start_consuming(self, on_message_callback):
		try:		
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
	def send(self, message_builder):
		try:
			self._send(message_builder.get_headers(), message_builder.serialize_payload())
		except routing.RoutingConnectionErrors as e:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at consume: {e}") from e
		except Exception as e:
			raise MessageMiddlewareMessageError(f"Message handling error: {e}") from e
	
	# Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
	# no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
	# Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.

	def stop_consuming(self):
		try:
			self.channel.stop_consuming()
		except routing.RoutingConnectionErrors as e:
			raise MessageMiddlewareDisconnectedError(f"RabbitMQ connection error at consume: {e}") from e

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
class RabbitExchangeMiddleware(MessageMiddlewareExchange):
	def __init__(self, host, exchange_name, route_keys):
		pass


