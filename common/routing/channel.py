

from .message_sender import MessageSender
from .message import Message
import logging

SELECT_TASKS_QUEUE_BASE = "select_tasks_queue"


def callback_wrapper(callback):
	def real_callback(ch, method, properties, body):
		logging.info(f"action: rcv_msg | result: success | method: {method} | props: {properties} | body:{body}")
		return callback(Message(ch, method, properties, body))
	return real_callback


class Channel:
	def __init__(self, rbmq_channel):
		self._rbmq_channel = rbmq_channel

	def consume_select_tasks(self, callback, auto_ack= False):
		self._rbmq_channel.queue_declare(queue=SELECT_TASKS_QUEUE_BASE)
		self._rbmq_channel.basic_consume(
		    queue=SELECT_TASKS_QUEUE_BASE, on_message_callback=callback_wrapper(callback), auto_ack=auto_ack)

	def open_sender_select_tasks(self):
		return MessageSender(self._rbmq_channel, SELECT_TASKS_QUEUE_BASE);

	def start_consume(self):
		self._rbmq_channel.start_consuming()