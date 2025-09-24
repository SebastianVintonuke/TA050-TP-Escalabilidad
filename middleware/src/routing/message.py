import logging

# Message builder
class MessageBuilder:
	def __init__(self,query_id, query_type):
		self.id = query_id
		self.type = query_type
		self.payload = []
		self.fields= []

	def set_fields(self, fields):
		# set field names!
		self.fields = fields

	def add_row(self,row):
		#assert len(row) == len(fields) # Same size of fields 
		self.payload.append(row)

	def build(self):
		return Message("", [self.id], [self.type], self.payload)

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

	def result_builder_for_single(self, ind):
		return MessageBuilder(self.ids[ind], self.types[ind])

	# For subclasses
	def ack_self(self):
		pass

	def describe(self):
		logging.info(f"action: msg_describe | result: success | tag: {self.tag} | queries_id:{self.ids} | queries_type: {self.types}")

	def stream_rows(self):
		logging.info(f"action: stream_rows | result: success | data: {self.payload}")