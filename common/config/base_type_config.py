import logging

from .row_mapping import (
    ROW_CONFIG_OUT_COLS,
    DictConvertWrapperMapper,
    NoActionRowMapper,
    RowMapper,
)


class BaseTypeConfiguration:

    def __init__(self, out_middleware, builder_creator, in_fields_count):
        self.middleware = out_middleware
        self.new_builder_for = builder_creator
        self.in_fields_count = in_fields_count  # ["year","month","some"] == 3 fields

        self.mapper = NoActionRowMapper()

    def load_mapper(self, config):
        self.mapper = RowMapper(config)

    def send(self, builder):
        return self.middleware.send(builder)

    # Filter map is in place.. do this first and pad just in case, for when wants to add cols
    def pad_copy_row(self, row):
        return row + [None] * (self.in_fields_count - len(row))

    def filter_map(self, row, msg_builder):
        try:
            return msg_builder.add_row(self.mapper(self.pad_copy_row(row)))
        except Exception as e:
            logging.error(f"Failed filter map of row {row} invalid {e}")
            return None


class BaseDictTypeConfiguration(BaseTypeConfiguration):

    def __init__(self, out_middleware, builder_creator, in_fields, out_conf=None):
        super().__init__(out_middleware, builder_creator, len(in_fields))
        if out_conf != None:
            self.load_mapper(out_conf)
            self.mapper = DictConvertWrapperMapper(
                in_fields, self.mapper, out_conf[ROW_CONFIG_OUT_COLS]
            )
        else:
            self.mapper = DictConvertWrapperMapper(in_fields, self.mapper, in_fields)

    # mapper does the mapping of input, not needed to pad.
    def filter_map(self, row, msg_builder):
        try:
            return msg_builder.add_row(self.mapper(self.mapper.map_input(row)))
        except Exception as e:
            logging.error(f"Failed filter map of row {row} invalid {e}")
            return None
