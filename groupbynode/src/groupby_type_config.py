
from common.config.row_mapping import DictConvertWrapperMapper,NoActionRowMapper, ROW_CONFIG_OUT_COLS

#from .row_grouping import load_grouper
from .row_aggregate import RowAggregator
from .row_key_parsing import *
import logging

GROUPED_KEY_FIELDS = 0
GROUPED_FIELDS_ACTIONS = 1

class GroupbyTypeConfiguration:
	def __init__(self, out_middleware, builder_creator, in_fields, grouping_conf, out_conf=None):
		self.middleware = out_middleware
		self.builder_creator = builder_creator

		self.mapper = DictConvertWrapperMapper(
			in_fields, NoActionRowMapper(), out_conf[ROW_CONFIG_OUT_COLS]
		)

		self.key_parser = KeyGroupParser(grouping_conf[GROUPED_KEY_FIELDS])
		self.grouper = RowAggregator(grouping_conf[GROUPED_FIELDS_ACTIONS])

	def map_input_row(self, row):
		return self.mapper.map_input(row)

	def add_output(self, msg_builder, group_key, acc):
		base = self.key_parser.get_base_key(group_key)
		self.grouper.add_aggregated_to(base, acc)
		
		msg_builder.add_row(self.mapper.project_out(base))



	def new_builder_for(self, inp_msg, ind_query):
		return self.builder_creator(inp_msg, ind_query)


	def send(self, builder):
		logging.info(f"GROUPBY SENDING TO {builder.types} {builder.ids} len: {builder.len_payload()} eof? {builder.is_eof()}")
		return self.middleware.send(builder)