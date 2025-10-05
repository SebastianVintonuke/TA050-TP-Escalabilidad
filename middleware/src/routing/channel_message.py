from .message import *

# Defines basic initialization from rabbitmq channel recv
class ChannelMessage(Message):
	def __init__(self, ch, method, headers, payload):
		super().__init__(headers, payload)
		self.ch =ch
		self.tag = method.delivery_tag

	def clone_with(self, queries_id, queries_type):
		return ChannelMessage(self.ch, self.tag, queries_id, queries_type, self.payload)

	def ack_self(self):
		self.ch.basic_ack(delivery_tag = self.tag)
