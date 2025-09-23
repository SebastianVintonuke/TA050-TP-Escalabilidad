from abc import ABC, abstractmethod
from typing import Callable, List

import pika
from pika.channel import Channel
from pika.exceptions import AMQPConnectionError
from pika.spec import Basic, BasicProperties


class MessageMiddlewareMessageError(Exception):
    pass


class MessageMiddlewareDisconnectedError(Exception):
    pass


class MessageMiddlewareCloseError(Exception):
    pass


class MessageMiddlewareDeleteError(Exception):
    pass


class MessageMiddleware(ABC):

    # Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
    # cada mensaje de datos o de control.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    @abstractmethod
    def start_consuming(
        self,
        on_message_callback: Callable[
            [Channel, Basic.Deliver, BasicProperties, bytes], None
        ],
    ) -> None:
        pass

    # Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
    # no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    @abstractmethod
    def stop_consuming(self) -> None:
        pass

    # Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    @abstractmethod
    def send(self, message: str | bytes) -> None:
        pass

    # Se desconecta de la cola o exchange al que estaba conectado.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    @abstractmethod
    def close(self) -> None:
        pass

    # Se fuerza la eliminación remota de la cola o exchange.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareDeleteError.
    @abstractmethod
    def delete(self) -> None:
        pass


class MessageMiddlewareExchange(MessageMiddleware):
    def __init__(self, host: str, exchange_name: str, route_keys: List[str]):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type="direct")
        result = self.channel.queue_declare(queue="", durable=True, exclusive=True)
        self.queue_name = result.method.queue
        self.channel.queue_bind(exchange=exchange_name, queue=self.queue_name)
        self.exchange_name = exchange_name
        self.route_keys = route_keys

    # Comienza a escuchar al exchange e invoca a on_message_callback tras
    # cada mensaje de datos o de control.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def start_consuming(
        self,
        on_message_callback: Callable[
            [Channel, Basic.Deliver, BasicProperties, bytes], None
        ],
    ) -> None:
        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=on_message_callback,
                auto_ack=False,
            )
            self.channel.start_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    # Si se estaba consumiendo desde el exchange, se detiene la escucha.
    # Si no se estaba consumiendo del exchange, no tiene efecto, ni levanta
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    def stop_consuming(self) -> None:
        try:
            self.channel.stop_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))

    # Envía un mensaje al tópico con el que se inicializó el exchange.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def send(self, message: str | bytes) -> None:
        try:
            for routing_key in self.route_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name, routing_key=routing_key, body=message
                )
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    # Se desconecta del exchange al que estaba conectado.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self) -> None:
        try:
            self.channel.close()
            self.connection.close()
        except Exception as error:
            raise MessageMiddlewareCloseError(str(error))

    # Se fuerza la eliminación remota del exchange.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareDeleteError.
    def delete(self) -> None:
        try:
            self.channel.queue_delete(queue=self.queue_name)
            self.channel.exchange_delete(exchange=self.exchange_name)
        except Exception as error:
            raise MessageMiddlewareDeleteError(str(error))


class MessageMiddlewareQueue(MessageMiddleware):
    def __init__(self, host: str, queue_name: str):
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name)

    # Comienza a escuchar a la cola e invoca a on_message_callback tras
    # cada mensaje de datos o de control.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def start_consuming(
        self,
        on_message_callback: Callable[
            [Channel, Basic.Deliver, BasicProperties, bytes], None
        ],
    ) -> None:
        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=on_message_callback,
                auto_ack=False,
            )
            self.channel.start_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    # Si se estaba consumiendo desde la cola, se detiene la escucha.
    # Si no se estaba consumiendo de la cola, no tiene efecto, ni levanta
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    def stop_consuming(self) -> None:
        try:
            self.channel.stop_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))

    # Envía un mensaje a la cola con la que se inicializó el exchange.
    # Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def send(self, message: str | bytes) -> None:
        try:
            self.channel.basic_publish(
                exchange="", routing_key=self.queue_name, body=message
            )
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    # Se desconecta de la cola a la que estaba conectada.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self) -> None:
        try:
            self.channel.close()
            self.connection.close()
        except Exception as error:
            raise MessageMiddlewareCloseError(str(error))

    # Se fuerza la eliminación remota de la cola.
    # Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareDeleteError.
    def delete(self) -> None:
        try:
            self.channel.queue_delete(queue=self.queue_name)
        except Exception as error:
            raise MessageMiddlewareDeleteError(str(error))
