import pika
import time
import logging

from .constants import *

logging.getLogger("pika").setLevel(logging.ERROR)

def build_headers(headers):
	return pika.BasicProperties(
				headers=headers
			)

def wait_middleware_init():
	time.sleep(5)

	
def try_open_connection(host,max_attempts):
	for i in range(1,max_attempts):
		try:
			return pika.BlockingConnection(pika.ConnectionParameters(
				host=host,
				heartbeat=600 * 4,  # 40 minutes hearbeat, should guarantee no conn resets
				blocked_connection_timeout=300,  # wait time before force close
				))
		except Exception as e:
			logging.warning(
				f"action: open_connection_middleware | result: in_progress | err:{e} | {i}"
			)
			time.sleep(5)

	# Last attempt
	return pika.BlockingConnection(pika.ConnectionParameters(host=host, 
				heartbeat=600 * 4,  # 40 minutes hearbeat, should guarantee no conn resets
				blocked_connection_timeout=300,  # wait time before force close
				))