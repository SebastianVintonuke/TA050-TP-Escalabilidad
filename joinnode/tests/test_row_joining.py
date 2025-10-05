import unittest
from common.config.row_joining import *
#import time
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

        # [ind , col]
        mapper = JoinProjectMapperOrdered(left_cols=[[0, 1]], right_cols=[[1, 0]])
        row_left = ["id_1", "Product"]
        row_right = ["Revenue", 100]

        result = mapper(row_left, row_right)
        self.assertEqual(result, ["Product", "Revenue"])


    def test_join_project_mapper_subset(self):

        # [ind , col]
        mapper = JoinProjectMapper(left_cols=[1], right_cols=[0])
        row_left = ["id_1", "Product"]
        row_right = ["Revenue", 100]

        result = mapper(row_left, row_right)
        self.assertEqual(result, ["Product", "Revenue"])

    def test_join_project_mapper_str_conversion(self):
        mapper = JoinProjectMapperOrdered([[1, 0]], [[0,1]])
        row_left = [123, "Ignore"]
        row_right = ["Ignore", 456.78]

        result = mapper(row_left, row_right)
        self.assertEqual(result, ["456.78","123"])  # All strings

    def test_join_project_mapper_str_conversion(self):
        mapper = JoinProjectMapperOrdered([[0, 0]], [[1,1]])
        row_left = [123, "Ignore"]
        row_right = ["Ignore", 456.78]
        result = mapper(row_left, row_right)
        self.assertEqual(result, ["123", "456.78"])  # All strings

    def test_join_project_mapper_str_conversion(self):
        mapper = JoinProjectMapper([0], [1])
        row_left = [123, "Ignore"]
        row_right = ["Ignore", 456.78]
        result = mapper(row_left, row_right)
        self.assertEqual(result, ["123", "456.78"])  # All strings


"""
    def test_bench_between_ordered_unordered(self):
        mapper = JoinProjectMapperOrdered([[0, 0]], [[1,1]])
        mapper2 = JoinProjectMapper([0], [1])

        rows_pairs=[
            ([123, "Ignore"], ["Ignore", 456.78]),
            ([125, "Ignor4e"], ["Ignor4e", 458.78]),
            ([126, "Ignore2"], ["Ignore2", 459.78]),
            ([124, "Ignore3"], ["Ignore3", 451.78]),
            ([123, "Ignore"], ["Ignore", 456.78]),
            ([125, "Ignor4e"], ["Ignor4e", 458.78]),
            ([126, "Ignore2"], ["Ignore2", 459.78]),
            ([124, "Ignore3"], ["Ignore3", 451.78]),
            ([123, "Ignore"], ["Ignore", 456.78]),
            ([125, "Ignor4e"], ["Ignor4e", 458.78]),
            ([126, "Ignore2"], ["Ignore2", 459.78]),
            ([124, "Ignore3"], ["Ignore3", 451.78]),
        ]
        times = 20000
        init_time = round(time.time()*1000)
        for i in range(times):
            for row_left, row_right in rows_pairs:
                result = mapper(row_left, row_right)
                self.assertEqual(result, [str(row_left[0]), str(row_right[1])])  # All strings
        end_time = round(time.time()*1000)
        print(f"ORDERED TOOK, {end_time-init_time}ms")

        init_time = round(time.time()*1000)
        for i in range(times):
            for row_left, row_right in rows_pairs:
                result = mapper2(row_left, row_right)
                self.assertEqual(result, [str(row_left[0]), str(row_right[1])])  # All strings
        end_time = round(time.time()*1000)
        print(f"UNORDERED TOOK, {end_time-init_time}ms")
        self.assertTrue(False)
"""
if __name__ == '__main__':
    unittest.main()
