


import pika
import time
import logging
from pika.exceptions import AMQPConnectionError, ConnectionClosed, ChannelClosed, StreamLostError


# Serial/message imports
from .message import *
from .message_sender import *
from .channel_message import *


logging.getLogger("pika").setLevel(logging.WARNING)

RABBITMQ_HOST = "middleware"
RoutingConnectionErrors = (
    AMQPConnectionError,
    ChannelClosed,
    ConnectionClosed,
    StreamLostError,
)

def try_open_connection(host,max_attempts):
	for i in range(1,max_attempts):
		try:
			return pika.BlockingConnection(pika.ConnectionParameters(host=host))
		except Exception as e:
			logging.warning(
				f"action: open_connection_middleware | result: in_progress | err:{e} | {i}"
			)
			time.sleep(1)

	# Last attempt
	return pika.BlockingConnection(pika.ConnectionParameters(host=host))