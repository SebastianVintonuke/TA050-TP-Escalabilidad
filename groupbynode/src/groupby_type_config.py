
from .row_grouping import load_grouper
from common.config.row_mapping import DictConvertWrapperMapper,NoActionRowMapper, ROW_CONFIG_OUT_COLS

class GroupbyTypeConfiguration:
	def __init__(self, out_middleware, builder_creator, in_fields, grouping_conf, out_conf=None):
		self.middleware = out_middleware
		self.builder_creator = builder_creator
		self.mapper = DictConvertWrapperMapper(
			in_fields, NoActionRowMapper(), out_conf[ROW_CONFIG_OUT_COLS]
		)

		self.grouper = load_grouper(grouping_conf)

	def map_input_row(self, row):
		return self.mapper.map_input(row)

	def get_output(self, group_key, acc):
		return self.mapper.project_out(self.grouper.expand_with_key(group_key, acc))


	def new_builder_for(self, inp_msg, ind_query):
		return self.builder_creator(inp_msg, ind_query)

	def send(self, builder):
		return self.middleware.send(builder)