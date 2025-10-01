


import pika
import time
import logging
from pika.exceptions import AMQPConnectionError, ConnectionClosed, ChannelClosed
from pika.exceptions import AMQPError, StreamLostError, ChannelClosedByBroker, ChannelWrongStateError, ConnectionWrongStateError

# Serial/message imports
from .message import *
from .message_sender import *
from .channel_message import *


logging.getLogger("pika").setLevel(logging.WARNING)

RABBITMQ_HOST = "middleware"
RoutingRestartError = (
	StreamLostError,
	ChannelClosedByBroker,
	ConnectionResetError,
	ConnectionWrongStateError,
	OSError,
	ChannelWrongStateError,
	AMQPError
)
RoutingConnectionErrors = (
    AMQPConnectionError,
    ChannelClosed,
    ConnectionClosed,
    StreamLostError,
)

def build_headers(headers):
	return pika.BasicProperties(
				headers=headers
			)

def wait_middleware_init():
	time.sleep(5)

	
def try_open_connection(host,max_attempts):
	for i in range(1,max_attempts):
		try:
			return pika.BlockingConnection(pika.ConnectionParameters(host=host))
		except Exception as e:
			logging.warning(
				f"action: open_connection_middleware | result: in_progress | err:{e} | {i}"
			)
			time.sleep(5)

	# Last attempt
	return pika.BlockingConnection(pika.ConnectionParameters(host=host))