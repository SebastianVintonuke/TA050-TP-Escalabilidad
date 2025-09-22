import logging


FIELD_QUERY_ID ="queries_id" 
FIELD_QUERY_TYPE ="queries_type" 

# Defines basic initialization and header management
class Message:
	def __init__(self, tag, queries_id, queries_type, payload):
		self.tag = tag
		self.ids = queries_id
		self.types = queries_type
		self.payload = payload
		# assert len of queries == len of types!
		assert len(self.ids) == len(self.types)

	def len_queries(self):
		return len(self.ids)

	def clone_with(self, queries_id, queries_type):
		return Message(self.tag, queries_id, queries_type, self.payload)

	# For subclasses
	def ack_self(self):
		pass

	def describe(self):
		logging.info(f"action: msg_describe | result: success | tag: {self.tag} | queries_id:{self.ids} | queries_type: {self.types}")

	def stream_rows(self):
		logging.info(f"action: stream_rows | result: success | data: {self.payload}")


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