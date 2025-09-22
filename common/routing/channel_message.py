from .message import Message
FIELD_QUERY_ID ="queries_id" 
FIELD_QUERY_TYPE ="queries_type" 

# Defines basic initialization from rabbitmq channel recv
class ChannelMessage(Message):
	def __init__(self, ch, method, headers, payload):
		super().__init__(method.delivery_tag, 
					headers.get(FIELD_QUERY_ID, []),
					headers.get(FIELD_QUERY_TYPE, []),
					payload)
		self.ch =ch

	def clone_with(self, queries_id, queries_type):
		return ChannelMessage(self.ch, self.tag, queries_id, queries_type, self.payload)

	def ack_self(self):
		self.ch.basic_ack(delivery_tag = self.tag)