import unittest
from selectnode.src.row_filtering import * 

class TestRowGrouper(unittest.TestCase):

    def test_less_than(self):
        self.assertFalse(False)

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
