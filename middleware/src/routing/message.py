import logging
#from .header_fields import *

def not_none(itm):
	return itm != None

# Defines basic initialization and header management
class Message:

	def __init__(self, payload):
		if payload:
			self.payload =payload
			self.empty = False
		else:
			self.payload = []
			self.empty = True
	def stream_rows(self):
		return self.payload
	def map_stream_rows(self, map_func):
		return filter(not_none, map(map_func, self.payload)) # payload is already a stream, assumed only will be iterated once.

	def has_payload(self):
		return not self.empty
	def is_empty(self):
		return self.empty