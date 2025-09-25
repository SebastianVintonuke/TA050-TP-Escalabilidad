
from .row_filtering import load_all_filters, should_keep

class TypeConfiguration:
	def __init__(self, type_conf, out_middleware, builder_creator):
		self.filters = load_all_filters(type_conf)
		self.middleware = out_middleware
		self.builder_creator = builder_creator

	def should_keep(self, row):
		return should_keep(self.filters, row)

	def new_builder_for(self, inp_msg, ind_query):
		return self.builder_creator(inp_msg, ind_query)

	def send(self, builder):
		return self.middleware.send(builder)
