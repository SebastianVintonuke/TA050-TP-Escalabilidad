
from .row_grouping import load_grouper

class TypeConfiguration:
	def __init__(self, type_conf, out_middleware, builder_creator):
		self.grouper = load_grouper(type_conf)
		self.middleware = out_middleware
		self.builder_creator = builder_creator

	def new_builder_for(self, inp_msg, ind_query):
		return self.builder_creator(inp_msg, ind_query)

	def send(self, builder):
		return self.middleware.send(builder)
