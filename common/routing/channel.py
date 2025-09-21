

from .message_sender import MessageSender

SELECT_TASKS_QUEUE_BASE = "select_tasks_queue"


# This Channel will encapsulate logic of serial too? Would be ideal prob
# Not always exactly possible?

class Channel:
	def __init__(self, rbmq_channel):
		self._rbmq_channel = rbmq_channel

	def consume_select_tasks(self, callback, auto_ack= False):
		self._rbmq_channel.queue_declare(queue=SELECT_TASKS_QUEUE_BASE)
		self._rbmq_channel.basic_consume(
		    queue=SELECT_TASKS_QUEUE_BASE, on_message_callback=callback, auto_ack=auto_ack)



	def open_sender_select_tasks(self):
		return MessageSender(self._rbmq_channel, SELECT_TASKS_QUEUE_BASE);

	def start_consume(self):
		self._rbmq_channel.start_consuming()