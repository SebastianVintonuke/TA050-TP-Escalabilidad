import logging


FIELD_QUERY_ID ="queries_id" 
FIELD_QUERY_TYPE ="queries_type" 
# Defines basic initialization and header management
class Message:
	def __init__(self, ch, method, properties, payload):
		self.tag = method.delivery_tag
		self.payload = payload # Might aswell deserialize it?
		self.ch =ch

		headers = properties.headers
		self.queries_id = headers.get(FIELD_QUERY_ID, [])
		self.queries_type = headers.get(FIELD_QUERY_TYPE, [])
		# assert len of queries == len of types!

	def ack_self(self):
		self.ch.basic_ack(delivery_tag = self.tag)

	def describe(self):
		logging.info(f"action: msg_describe | result: success | tag: {self.tag} | queries_id:{self.queries_id} | queries_type: {self.queries_type}")
