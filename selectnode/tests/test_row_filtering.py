import unittest

from common.config.row_filtering import *


class TestRowFilter(unittest.TestCase):

    def test_less_than(self):
        rf = RowFilter("age", LESSER_THAN_OP, [30])
        self.assertTrue(rf.should_keep({"age": 25}))
        self.assertFalse(rf.should_keep({"age": 35}))

    def test_greater_than(self):
        rf = RowFilter("score", GREATER_THAN_OP, [70])
        self.assertTrue(rf.should_keep({"score": 85}))
        self.assertFalse(rf.should_keep({"score": 60}))

    def test_less_eq_than(self):
        rf = RowFilter("age", LESSER_EQ_THAN_OP, [30])
        self.assertTrue(rf.should_keep({"age": 30}))
        self.assertTrue(rf.should_keep({"age": 25}))
        self.assertFalse(rf.should_keep({"age": 35}))

    def test_greater_eq_than(self):
        rf = RowFilter("score", GREATER_EQ_THAN_OP, [70])
        self.assertTrue(rf.should_keep({"score": 70}))
        self.assertFalse(rf.should_keep({"score": 60}))
        self.assertTrue(rf.should_keep({"score": 85}))

    def test_between(self):
        rf = RowFilter("height", BETWEEN_THAN_OP, [150, 180])
        self.assertTrue(rf.should_keep({"height": 160}))
        self.assertFalse(rf.should_keep({"height": 145}))

    def test_equal_any(self):
        rf = RowFilter("status", EQUALS_ANY, ["active", "pending"])
        self.assertTrue(rf.should_keep({"status": "active"}))
        self.assertTrue(rf.should_keep({"status": "pending"}))
        self.assertFalse(rf.should_keep({"status": "disabled"}))

    def test_not_equals(self):
        rf = RowFilter("role", NOT_EQUALS, ["admin", "superuser"])
        self.assertFalse(
            rf.should_keep({"role": "admin"})
        )  # All must be != — this fails
        self.assertTrue(rf.should_keep({"role": "guest"}))  # guest != all, so it passes

    def test_load_all_filters(self):
        filters_serial = [
            ["age", GREATER_THAN_OP, [20]],
            ["status", EQUALS_ANY, ["active"]],
        ]
        filters = load_all_filters(filters_serial)

        self.assertEqual(len(filters), 2)
        self.assertIsInstance(filters[0], RowFilter)
        self.assertTrue(filters[0].should_keep({"age": 25}))

    def test_should_keep_multiple_filters(self):
        filters_serial = [
            ["age", GREATER_THAN_OP, [18]],
            ["country", EQUALS_ANY, ["US", "CA"]],
        ]
        filters = load_all_filters(filters_serial)

        row_pass = {"age": 20, "country": "US"}
        row_fail = {"age": 17, "country": "US"}
        row_fail2 = {"age": 20, "country": "FR"}

        self.assertTrue(should_keep(filters, row_pass))
        self.assertFalse(should_keep(filters, row_fail))
        self.assertFalse(should_keep(filters, row_fail2))

    def test_invalid_operator_raises(self):
        with self.assertRaises(AssertionError):
            RowFilter("foo", "invalid_op", ["x"])

    def test_query_1_usecase(self):
        filters_serial = [
            ["year", EQUALS_ANY, [2024, 2025]],
            ["hour", BETWEEN_THAN_OP, [6, 23]],
            ["sum", GREATER_THAN_OP, [75]],
        ]

        filters = load_all_filters(filters_serial)

        rows_pass = [
            {"year": 2024, "hour": 7, "sum": 88},
            {"year": 2025, "hour": 23, "sum": 942},
            {"year": 2024, "hour": 6, "sum": 942},
        ]
        rows_fail = [
            {"year": 2027, "hour": 7, "sum": 88},
            {"year": 2025, "hour": 24, "sum": 942},
            {"year": 2024, "hour": 6, "sum": 55},
        ]

        for row in rows_pass:
            self.assertTrue(should_keep(filters, row))

        for row in rows_fail:
            self.assertFalse(should_keep(filters, row))

    def test_query_1_usecase_compiled(self):
        filters_serial = [
            ["year", EQUALS_ANY, [2024, 2025]],
            ["hour", BETWEEN_THAN_OP, [6, 23]],
            ["sum", GREATER_THAN_OP, [75]],
        ]

        filter_compiled = build_filter_from_config(filters_serial)

        rows_pass = [
            {"year": 2024, "hour": 7, "sum": 88},
            {"year": 2025, "hour": 23, "sum": 942},
            {"year": 2024, "hour": 6, "sum": 942},
        ]
        rows_fail = [
            {"year": 2027, "hour": 7, "sum": 88},
            {"year": 2025, "hour": 24, "sum": 942},
            {"year": 2024, "hour": 6, "sum": 55},
        ]

        for row in rows_pass:
            self.assertTrue(filter_compiled.should_keep(row))

        for row in rows_fail:
            self.assertFalse(filter_compiled.should_keep(row))




"""
        types_config = {
            "query_1": [
                ["year", EQUALS_ANY, [2024, 2025]],
                ["hour", BETWEEN_THAN_OP, [6, 23]],
                ["sum", GREATER_THAN_OP, [75]],
            ],
            "query_2": [
                ["year", EQUALS_ANY, [2024, 2025]],
            ],
            "query_3": [
                ["year", EQUALS_ANY, [2024, 2025]],
                ["hour", BETWEEN_THAN_OP, [6, 23]],
            ],
            "query_4": [
                ["year", EQUALS_ANY, [2024, 2025]],
            ],
        }

"""
