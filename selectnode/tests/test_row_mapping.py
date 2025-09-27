import unittest
from selectnode.src.row_mapping import * 

class TestRowMapper(unittest.TestCase):
    def test_map_month(self):
        config = {
            "actions": [
                [MAP_MONTH, {
                    "init_year": 2020,
                    "col_year": "year",
                    "col_month": "month",
                    "col_out": "mapped_month"
                }]
            ],
            "out_cols": ["mapped_month"]
        }
        mapper = RowMapper(config)
        row = {"year": "2021", "month": "3"}
        output = mapper(row)
        self.assertEqual(output, ["15"])  # (2021 - 2020) * 12 + 3 = 15

    def test_map_semester(self):
        config = {
            "actions": [
                [MAP_SEMESTER, {
                    "init_year": 2020,
                    "col_year": "year",
                    "col_month": "month",
                    "col_out": "mapped_semester"
                }]
            ],
            "out_cols": ["mapped_semester"]
        }
        mapper = RowMapper(config)
        row = {"year": "2021", "month": "6"}
        output = mapper(row)
        self.assertEqual(output, ["6"])  # months= (1*12+6)=18 -> 18//3 = 6

    def test_combined_map(self):
        config = {
            "actions": [
                [MAP_MONTH, {
                    "init_year": 2020,
                    "col_year": "year",
                    "col_month": "month",
                    "col_out": "mapped_month"
                }],
                [MAP_SEMESTER, {
                    "init_year": 2020,
                    "col_year": "year",
                    "col_month": "month",
                    "col_out": "mapped_semester"
                }]
            ],
            "out_cols": ["mapped_month", "mapped_semester"]
        }
        mapper = RowMapper(config)
        row = {"year": "2021", "month": "3"}
        output = mapper(row)
        self.assertEqual(output, ["15", "5"])  # 15 months -> semester = 5

    def test_edge_case_start(self):
        config = {
            "actions": [
                [MAP_MONTH, {
                    "init_year": 2020,
                    "col_year": "year",
                    "col_month": "month",
                    "col_out": "mapped_month"
                }]
            ],
            "out_cols": ["mapped_month"]
        }
        mapper = RowMapper(config)
        row = {"year": "2020", "month": "0"}
        output = mapper(row)
        self.assertEqual(output, ["0"])

    def test_missing_column(self):
        config = {
            "actions": [
                [MAP_MONTH, {
                    "init_year": 2020,
                    "col_year": "year",
                    "col_month": "month",
                    "col_out": "mapped_month"
                }]
            ],
            "out_cols": ["mapped_month"]
        }
        mapper = RowMapper(config)
        row = {"year": "2020"}  # missing 'month'

        with self.assertRaises(KeyError):
            mapper(row)

if __name__ == "__main__":
    unittest.main()
