import unittest

from middleware.mocks.middleware import *

from common.config.base_type_config import *
from common.config.row_filtering import *
from common.config.row_mapping import *
from selectnode.src.select_type_config import *


# ["year", "month", "hour","product_id"]
def map_dict_to_vect(row):
    return [row["year"], row["month"], row["hour"], row["product_id"]]


# ["product_id","hour","mapped_month"]
def map_vect_to_dict(row):
    return {"product_id": row[0], "hour": int(row[1]), "mapped_month": int(row[2])}


class TestTypeConfigs(unittest.TestCase):
    def build_simple_base_config(self, result_grouper):
        in_fields_count = 4  # ["year", "month", "hour","product_id"] == 4
        out_cols = [3, 2, 1]  # ["product_id","hour","mapped_month"]

        config = {
            ROW_CONFIG_ACTIONS: [
                [
                    MAP_MONTH,
                    {"init_year": 2020, "col_year": 0, "col_month": 1, "col_out": 1},
                ]
            ],
            ROW_CONFIG_OUT_COLS: out_cols,
        }
        config_type = BaseTypeConfiguration(
            result_grouper, MockMessageBuilder, in_fields_count=in_fields_count
        )
        config_type.load_mapper(config)
        return config_type

    # Add a new col instead of using 1 col for mapped output
    def build_simple_base_config_new_cols(self, result_grouper):
        in_fields_count = 5  # ["year", "month", "hour","product_id"] == 4
        out_cols = [3, 2, 4]  # ["product_id","hour","mapped_month"]

        config = {
            ROW_CONFIG_ACTIONS: [
                [
                    MAP_MONTH,
                    {"init_year": 2020, "col_year": 0, "col_month": 1, "col_out": 4},
                ]
            ],
            ROW_CONFIG_OUT_COLS: out_cols,
        }
        config_type = BaseTypeConfiguration(
            result_grouper, MockMessageBuilder, in_fields_count=in_fields_count
        )
        config_type.load_mapper(config)
        return config_type

    def build_simple_base_dict_config(self, result_grouper):
        in_fields = ["year", "month", "hour", "product_id"]
        out_cols = ["product_id", "hour", "mapped_month"]

        config = {
            ROW_CONFIG_ACTIONS: [
                [
                    MAP_MONTH,
                    {
                        "init_year": 2020,
                        "col_year": "year",
                        "col_month": "month",
                        "col_out": "mapped_month",
                    },
                ]
            ],
            ROW_CONFIG_OUT_COLS: out_cols,
        }
        config_type = BaseDictTypeConfiguration(
            result_grouper, MockMessageBuilder, in_fields=in_fields, out_conf=config
        )
        return config_type

    def test_base_type_config(self):
        result_grouper = MockMiddleware()
        config = self.build_simple_base_config(result_grouper)
        # ["year", "month", "hour", "product_id"]
        rows = [
            {"year": 2024, "hour": 7, "month": 11, "product_id": "prod_1"},
            {"year": 2020, "hour": 8, "month": 0, "product_id": "prod_2"},
        ]

        out_rows = []
        for row in rows:
            out_rows.append(config.filter_map(map_dict_to_vect(row)))

        self.assertEqual(len(out_rows), len(rows))

        self.assertEqual(out_rows[0], ["prod_1", "7", "59"])  # 59 == 12* 4 + 11 = 59
        self.assertEqual(out_rows[1], ["prod_2", "8", "0"])  # 0 == 12* 0 + 0 = 0

    def build_simple_select_config(self, result_grouper, filters_serial):
        in_fields = ["year", "month", "hour", "product_id"]
        out_cols = ["product_id", "hour", "mapped_month"]

        config = {
            ROW_CONFIG_ACTIONS: [
                [
                    MAP_MONTH,
                    {
                        "init_year": 2020,
                        "col_year": "year",
                        "col_month": "month",
                        "col_out": "mapped_month",
                    },
                ]
            ],
            ROW_CONFIG_OUT_COLS: out_cols,
        }

        config_type = SelectTypeConfiguration(
            result_grouper,
            MockMessageBuilder,
            in_fields=in_fields,
            filters_conf=filters_serial,
            out_conf=config,
        )

        return config_type

    def test_base_extend_type_config(self):
        result_grouper = MockMiddleware()
        config = self.build_simple_base_config_new_cols(result_grouper)
        # ["year", "month", "hour", "product_id"]
        rows = [
            {"year": 2024, "hour": 7, "month": 11, "product_id": "prod_1"},
            {"year": 2020, "hour": 8, "month": 0, "product_id": "prod_2"},
        ]

        out_rows = []
        for row in rows:
            out_rows.append(config.filter_map(map_dict_to_vect(row)))

        self.assertEqual(len(out_rows), len(rows))

        self.assertEqual(out_rows[0], ["prod_1", "7", "59"])  # 59 == 12* 4 + 11 = 59
        self.assertEqual(out_rows[1], ["prod_2", "8", "0"])  # 0 == 12* 0 + 0 = 0

    def test_base_dict_type_config(self):
        result_grouper = MockMiddleware()
        config = self.build_simple_base_dict_config(result_grouper)
        # ["year", "month", "hour", "product_id"]
        rows = [
            {"year": 2024, "hour": 7, "month": 11, "product_id": "prod_1"},
            {"year": 2020, "hour": 8, "month": 0, "product_id": "prod_2"},
        ]

        out_rows = []
        for row in rows:
            out_rows.append(config.filter_map(map_dict_to_vect(row)))

        self.assertEqual(len(out_rows), len(rows))

        self.assertEqual(out_rows[0], ["prod_1", "7", "59"])  # 59 == 12* 4 + 11 = 59
        self.assertEqual(out_rows[1], ["prod_2", "8", "0"])  # 0 == 12* 0 + 0 = 0

    def test_select_type_config(self):
        result_grouper = MockMiddleware()

        filters_serial = [
            ["year", EQUALS_ANY, [2024, 2025]],
            ["hour", BETWEEN_THAN_OP, [6, 23]],
        ]

        config = self.build_simple_select_config(result_grouper, filters_serial)
        # ["year", "month", "hour", "product_id"]
        rows = [
            {"year": 2024, "hour": 7, "month": 11, "product_id": "prod_1"},
            {"year": 2020, "hour": 8, "month": 0, "product_id": "prod_2"},
            {"year": 2025, "hour": 22, "month": 0, "product_id": "prod_2"},
            {"year": 2025, "hour": 24, "month": 0, "product_id": "prod_2"},
        ]

        out_rows = []
        for row in rows:
            out_rows.append(config.filter_map(map_dict_to_vect(row)))

        self.assertEqual(len(out_rows), len(rows))
        # Len is still the same but filled with nones when filtered
        #    {'year': 2024, 'hour': 7, 'month': 11, "product_id":"prod_1"},
        #    {'year': 2025, 'hour': 22, 'month': 0, "product_id":"prod_2"},
        # this two columns were processed,
        # [['prod_1', '7', '59'], None, ['prod_2', '22', '60'], None]

        self.assertEqual(out_rows[0], ["prod_1", "7", "59"])  # 59 == 12* 4 + 11 = 59
        self.assertEqual(out_rows[1], None)  # filtered
        self.assertEqual(out_rows[2], ["prod_2", "22", "60"])  # 60 == 12* 5 + 0 = 60
        self.assertEqual(out_rows[3], None)  # filtered
