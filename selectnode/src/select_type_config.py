import logging

from .base_type_config import BaseDictTypeConfiguration, BaseTypeConfiguration
from .row_filtering import load_all_filters, should_keep


class SelectTypeConfiguration(BaseDictTypeConfiguration):

    # def __init__(self, out_middleware, builder_creator, in_fields_count):
    # def __init__(self, out_middleware, builder_creator, in_fields, out_conf = None):
    def __init__(
        self, out_middleware, builder_creator, in_fields, filters_conf, out_conf=None
    ):
        super().__init__(out_middleware, builder_creator, in_fields, out_conf)
        self.filters = load_all_filters(filters_conf)

    def should_keep(self, row):
        return should_keep(self.filters, row)

    def filter_map(self, row):
        try:
            # No pad needed ? self.pad_copy_row(row), mapping to dict and if not enough rows... fail
            row = self.mapper.map_input(row)
            if self.should_keep(row):
                return self.mapper(row)
            return None
        except Exception as e:
            logging.error(f"Failed filter map of row {row} invalid {e}")
            return None
