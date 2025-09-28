import unittest

from common.config.row_mapping import *


class TestRowMappers(unittest.TestCase):
    def test_dict_convert_wrapper_handles_input_mapping_and_projects(self):
        in_cols = ["a", "b", "c"]
        out_cols = ["a", "c"]
        wrapper = DictConvertWrapperMapper(
            in_cols=in_cols, inner_mapper=NoActionRowMapper(), out_cols=out_cols
        )
        vec = ["x", "y", "z"]
        dict_row = wrapper.map_input(vec)
        self.assertEqual(dict_row, {"a": "x", "b": "y", "c": "z"})
        out = wrapper.project_out(dict_row)
        self.assertEqual(out, ["x", "z"])

    def test_dict_row_mapper_map_month(self):
        in_cols = ["year", "month"]
        out_cols = ["mapped_month"]
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
        mapper = DictConvertWrapperMapper(
            in_cols=in_cols, inner_mapper=RowMapper(config), out_cols=out_cols
        )
        row = {"year": "2021", "month": "3"}
        output = mapper(row)
        self.assertEqual(output, ["15"])  # (1 * 12 + 3)

    def test_dict_row_mapper_map_month_reorder_cols(self):
        in_cols = ["year", "month", "store_id", "user_id"]
        out_cols = ["store_id", "user_id", "mapped_month"]

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

        mapper = DictConvertWrapperMapper(
            in_cols=in_cols, inner_mapper=RowMapper(config), out_cols=out_cols
        )
        row = ["2021", "3", "store_1", "user_2"]
        output = mapper(mapper.map_input(row))
        self.assertEqual(output, ["store_1", "user_2", "15"])  # (1 * 12 + 3)

    def test_dict_row_mapper_map_semester(self):
        in_cols = ["year", "month"]
        out_cols = ["mapped_semester"]
        config = {
            ROW_CONFIG_ACTIONS: [
                [
                    MAP_SEMESTER,
                    {
                        "init_year": 2020,
                        "col_year": "year",
                        "col_month": "month",
                        "col_out": "mapped_semester",
                    },
                ]
            ],
            ROW_CONFIG_OUT_COLS: out_cols,
        }
        mapper = DictConvertWrapperMapper(
            in_cols=in_cols, inner_mapper=RowMapper(config), out_cols=out_cols
        )

        row = {"year": "2022", "month": "6"}
        output = mapper(row)
        self.assertEqual(output, ["10"])  # (2 * 12 + 6) // 3 = 30 // 3 = 10

    def test_vector_input_with_dict_convert_wrapper(self):
        in_cols = ["year", "month"]
        out_cols = ["mapped_month"]

        config = {
            ROW_CONFIG_ACTIONS: [
                [
                    MAP_MONTH,
                    {
                        "init_year": 2021,
                        "col_year": "year",
                        "col_month": "month",
                        "col_out": "mapped_month",
                    },
                ]
            ],
            ROW_CONFIG_OUT_COLS: out_cols,
        }
        # Inner mapper expects a dict
        inner_mapper = RowMapper(config)
        # Outer wrapper allows vector input
        wrapper = DictConvertWrapperMapper(
            in_cols=in_cols, inner_mapper=inner_mapper, out_cols=out_cols
        )
        vector_row = ["2022", "5"]

        output = wrapper(wrapper.map_input(vector_row))
        self.assertEqual(output, ["17"])  # (1*12 + 5)

    def test_row_project_mapper_with_dict(self):
        in_cols = ["name", "age", "extra"]
        out_cols = ["name", "age"]
        mapper = DictConvertWrapperMapper(
            in_cols=in_cols, inner_mapper=RowProjectMapper(out_cols), out_cols=out_cols
        )

        row = {"name": "Alice", "age": 30, "extra": "ignore_me"}
        output = mapper(row)
        self.assertEqual(output, ["Alice", "30"])

    def test_row_project_mapper_with_vector(self):
        mapper = RowProjectMapper([0, 2])
        row = ["John", "Doe", 25]
        output = mapper(row)
        self.assertEqual(output, ["John", "25"])

    def test_mapping_is_inplace_on_dict(self):
        in_cols = ["year", "month"]
        out_cols = ["mapped"]
        config = {
            ROW_CONFIG_ACTIONS: [
                [
                    MAP_MONTH,
                    {
                        "init_year": 2020,
                        "col_year": "year",
                        "col_month": "month",
                        "col_out": "mapped",
                    },
                ]
            ],
            ROW_CONFIG_OUT_COLS: out_cols,
        }
        mapper = DictConvertWrapperMapper(
            in_cols=in_cols, inner_mapper=RowMapper(config), out_cols=out_cols
        )

        row = {"year": "2021", "month": "1"}
        mapper.map(row)
        self.assertEqual(row["mapped"], 13)

    def test_mapping_is_inplace_on_vec(self):
        out_cols = [0, 2]
        config = {
            ROW_CONFIG_ACTIONS: [
                [
                    MAP_MONTH,
                    {"init_year": 2020, "col_year": 0, "col_month": 1, "col_out": 2},
                ]
            ],
            ROW_CONFIG_OUT_COLS: out_cols,
        }
        mapper = RowMapper(config)

        row = ["2021", "1", None]  # Needed to fill in
        mapper.map(row)
        self.assertEqual(row[2], 13)

    def test_mapping_is_inplace_override(self):
        out_cols = [0, 1]
        config = {
            ROW_CONFIG_ACTIONS: [
                [
                    MAP_MONTH,
                    {"init_year": 2020, "col_year": 0, "col_month": 1, "col_out": 1},
                ]
            ],
            ROW_CONFIG_OUT_COLS: out_cols,
        }
        mapper = RowMapper(config)

        row = ["2021", "0"]
        mapper.map(row)
        self.assertEqual(row[1], 12)
        mapper.map(row)
        self.assertEqual(row[1], 24)

    def test_mapping_project_on_vec(self):
        out_cols = [0, 2]
        config = {
            ROW_CONFIG_ACTIONS: [
                [
                    MAP_MONTH,
                    {"init_year": 2020, "col_year": 0, "col_month": 1, "col_out": 2},
                ]
            ],
            ROW_CONFIG_OUT_COLS: out_cols,
        }
        mapper = RowMapper(config)

        row = ["2021", "1", None]  # Needed to fill in
        res = mapper(row)
        self.assertEqual(res, ["2021", "13"])

    def test_edge_case_zero_month(self):
        out_cols = [0]
        config = {
            ROW_CONFIG_ACTIONS: [
                [
                    MAP_MONTH,
                    {"init_year": 2020, "col_year": 0, "col_month": 1, "col_out": 0},
                ]
            ],
            ROW_CONFIG_OUT_COLS: out_cols,
        }
        mapper = RowMapper(config)
        row = ["2020", "0"]
        output = mapper(row)
        self.assertEqual(output, ["0"])

    def test_missing_in_column_raises_indexerror(self):
        config = {
            ROW_CONFIG_ACTIONS: [
                [
                    MAP_SEMESTER,
                    {"init_year": 2021, "col_year": 0, "col_month": 1, "col_out": 2},
                ]
            ],
            ROW_CONFIG_OUT_COLS: [0, 1],
        }
        mapper = RowMapper(config)
        row = ["2022"]  # 'month' is missing
        with self.assertRaises(IndexError):
            mapper(row)

    def test_missing_in_column_raises_keyerror_dict(self):
        in_cols = ["year", "month"]
        out_cols = ["mapped_month"]

        config = {
            ROW_CONFIG_ACTIONS: [
                [
                    MAP_MONTH,
                    {
                        "init_year": 2021,
                        "col_year": "year",
                        "col_month": "month",
                        "col_out": "mapped_month",
                    },
                ]
            ],
            ROW_CONFIG_OUT_COLS: out_cols,
        }
        # Inner mapper expects a dict
        inner_mapper = RowMapper(config)
        # Outer wrapper allows vector input
        wrapper = DictConvertWrapperMapper(
            in_cols=in_cols, inner_mapper=inner_mapper, out_cols=out_cols
        )
        row = {"year": "2022"}  # 'month' is missing
        with self.assertRaises(KeyError):
            wrapper(row)

    def test_missing_out_column_raises_indexerror(self):
        config = {
            ROW_CONFIG_ACTIONS: [
                [
                    MAP_SEMESTER,
                    {"init_year": 2021, "col_year": 0, "col_month": 1, "col_out": 2},
                ]
            ],
            ROW_CONFIG_OUT_COLS: [0, 1],
        }
        mapper = RowMapper(config)
        row = [
            "2022",
            "1",
        ]  # mapped_month is missing, need to fill in with None or something If using index 2
        with self.assertRaises(IndexError):
            mapper(row)
