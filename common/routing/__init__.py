
import pika
from .channel import Channel
import time
import logging

RABBITMQ_HOST = "middleware"
connection = None

class Connection:
	def __init__(self, host = RABBITMQ_HOST):
		self._conn = pika.BlockingConnection(
		    pika.ConnectionParameters(host=RABBITMQ_HOST))

	def open_channel(self):	
		return Channel(self._conn.channel())



def try_open_connection(max_attempts = 10, host = RABBITMQ_HOST):
	for i in range(1,max_attempts):
		try:
			return Connection(host)
		except Exception as e:
			logging.warning(
				f"action: open_connection_middleware | result: in_progress | err:{e} | {i}"
			)
			time.sleep(1)

	# Last attempt
	return Connection(host)
