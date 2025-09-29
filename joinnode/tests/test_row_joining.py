import unittest
from common.config.row_joining import *

class TestInnerEqualJoin(unittest.TestCase):
    def test_should_join_true(self):
        joiner = InnerEqualJoin(0, 1)
        self.assertTrue(joiner.should_join(["A"], ["B", "A"]))

    def test_should_join_false_1(self):
        joiner = InnerEqualJoin(0, 0)
        self.assertFalse(joiner.should_join(["A"], ["B", "A"]))

    def test_should_join_false(self):
        joiner = InnerEqualJoin(0, 1)
        self.assertFalse(joiner.should_join(["A"], ["B", "C"]))

    def test_map_cols(self):
        joiner = InnerEqualJoin("left_id", "right_id")
        joiner.map_cols(lambda x: 0 if x == "left_id" else -1,
                        lambda x: 1 if x == "right_id" else -1)

        self.assertEqual(joiner.col_left, 0)
        self.assertEqual(joiner.col_right, 1)

        self.assertTrue(joiner.should_join(["match"], ["X", "match"]))
        self.assertFalse(joiner.should_join(["nope"], ["X", "match"]))

    def test_load_joiner(self):
        config = [INNER_ON_EQ, {"col_left": 0, "col_right": 1}]
        joiner = load_joiner(config)

        self.assertIsInstance(joiner, InnerEqualJoin)
        self.assertTrue(joiner.should_join(["match"], ["X", "match"]))


class TestJoinProjectMappers(unittest.TestCase):
    def test_join_project_mapper_all(self):
        mapper = JoinProjectMapperAll()
        result = mapper(["a", "b"], ["c", "d"])
        self.assertEqual(result, ["a", "b", "c", "d"])

    def test_join_project_mapper_subset(self):
        mapper = JoinProjectMapper(left_cols=[1], right_cols=[0])
        row_left = ["id_1", "Product"]
        row_right = ["Revenue", 100]

        result = mapper(row_left, row_right)
        self.assertEqual(result, ["Product", "Revenue"])

    def test_join_project_mapper_str_conversion(self):
        mapper = JoinProjectMapper([0], [1])
        row_left = [123, "Ignore"]
        row_right = ["Ignore", 456.78]

        result = mapper(row_left, row_right)
        self.assertEqual(result, ["123", "456.78"])  # All strings

if __name__ == '__main__':
    unittest.main()
