import unittest
from groupbynode.src.row_grouping import * 

import unittest

class TestRowGrouper(unittest.TestCase):

    def test_sum_action_with_zero_and_negatives(self):
        fields_grouped_by = ["region"]
        fields_acc = {
            "sales": SUM_ACTION
        }

        grouper = RowGrouper(fields_grouped_by, fields_acc)

        row1 = {"region": "north", "sales": 0}
        acc = grouper.new_group_acc(row1)

        row2 = {"region": "north", "sales": -10}
        grouper.add_group_acc(acc, row2)

        row3 = {"region": "north", "sales": 15}
        grouper.add_group_acc(acc, row3)

        self.assertEqual(acc["sales"], 5)

    def test_max_action(self):
        fields_grouped_by = ["user"]
        fields_acc = {
            "score": MAX_ACTION
        }

        grouper = RowGrouper(fields_grouped_by, fields_acc)

        row1 = {"user": "A", "score": 10}
        acc = grouper.new_group_acc(row1)

        row2 = {"user": "A", "score": 30}
        grouper.add_group_acc(acc, row2)

        row3 = {"user": "A", "score": 20}
        grouper.add_group_acc(acc, row3)

        self.assertEqual(acc["score"], 30)

    def test_avg_action(self):
        fields_grouped_by = ["product"]
        fields_acc = {
            "price": AVG_ACTION
        }

        grouper = RowGrouper(fields_grouped_by, fields_acc)

        row1 = {"product": "apple", "price": 10}
        acc = grouper.new_group_acc(row1)
        row2 = {"product": "apple", "price": 20}
        grouper.add_group_acc(acc, row2)

        row3 = {"product": "apple", "price": 30}
        grouper.add_group_acc(acc, row3)

        count, avg = acc["price"]
        self.assertEqual(count, 3)
        # approximate float equality
        self.assertAlmostEqual(avg, 20.0)

        res = grouper.expand_with_key(
            grouper.get_group_key(row1),
            acc
        )
        self.assertEqual(res, {"product":"apple", "price":20.0})


    def test_count_only(self):
        fields_grouped_by = ["category"]
        fields_acc = {
            "views": COUNT_ACTION
        }

        grouper = RowGrouper(fields_grouped_by, fields_acc)

        row1 = {"category": "tech", "views": 100}
        acc = grouper.new_group_acc(row1)

        row2 = {"category": "tech", "views": 999}
        grouper.add_group_acc(acc, row2)

        self.assertEqual(acc["views"], 2)

    def test_missing_field_raises(self):
        fields_grouped_by = ["country"]
        fields_acc = {
            "cost": SUM_ACTION
        }

        grouper = RowGrouper(fields_grouped_by, fields_acc)

        row1 = {"country": "USA"}  # Missing 'cost'

        with self.assertRaises(KeyError):
            grouper.new_group_acc(row1)

    def test_empty_data_does_not_fail(self):
        fields_grouped_by = ["x"]
        fields_acc = {
            "value": SUM_ACTION
        }

        grouper = RowGrouper(fields_grouped_by, fields_acc)

        row1 = {"x": 1, "value": 0}
        acc = grouper.new_group_acc(row1)
        self.assertEqual(acc["value"], 0)

    def test_mixed_data_types_handled(self):
        fields_grouped_by = ["group"]
        fields_acc = {
            "val": SUM_ACTION
        }

        grouper = RowGrouper(fields_grouped_by, fields_acc)

        row1 = {"group": "A", "val": 5}
        acc = grouper.new_group_acc(row1)

        # Passing a string instead of a number should raise
        row2 = {"group": "A", "val": "oops"}

        with self.assertRaises(TypeError):
            grouper.add_group_acc(acc, row2)

    def test_multiple_adds(self):
        fields_grouped_by = ["group"]
        fields_acc = {
            "val": SUM_ACTION,
            "num": COUNT_ACTION
        }

        grouper = RowGrouper(fields_grouped_by, fields_acc)

        row = {"group": "B", "val": 1}
        acc = grouper.new_group_acc(row)

        for i in range(4):
            grouper.add_group_acc(acc, {"group": "B", "val": 2})

        self.assertEqual(acc["val"], 1 + 4 * 2)
        self.assertEqual(acc["num"], 1 + 4)  # starts at 1 in new_group_acc

    def test_group_key_is_tuple(self):
        fields_grouped_by = ["year", "region"]
        fields_acc = {
            "revenue": SUM_ACTION
        }

        grouper = RowGrouper(fields_grouped_by, fields_acc)

        row = {"year": 2025, "region": "EU", "revenue": 100}
        key = grouper.get_group_key(row)
        self.assertEqual(key, (2025, "EU"))

    def test_simple_two_groups_have_diff_keys(self):
        fields_grouped_by = ["year", "store_id", "user_id"]
        fields_acc = {
                "cost": SUM_ACTION,
                "purchase_count": COUNT_ACTION
        }

        grouper = RowGrouper(fields_grouped_by, fields_acc)

        row1 = {"year":2025,"store_id":1,"user_id":1,"cost":12}

        key1 = grouper.get_group_key(row1)

        row2 = {"year":2025,"store_id":2,"user_id":1,"cost":12}
        
        key2 = grouper.get_group_key(row2)

        row3 = {"year":2025,"store_id":1,"user_id":1,"cost":54}
        key3 = grouper.get_group_key(row3)

        self.assertEqual(key1,key3)
        self.assertFalse(key1 == key2)

    def test_simple_two_row_group_sum_and_count(self):
        fields_grouped_by = ["year", "store_id", "user_id"]
        fields_acc = {
                "cost": SUM_ACTION,
                "purchase_count": COUNT_ACTION
        }

        grouper = RowGrouper(fields_grouped_by, fields_acc)



        row1 = {"year":2025,"store_id":1,"user_id":1,"cost":12}

        key = grouper.get_group_key(row1)
        self.assertEqual(key, (2025,1,1))

        acc = grouper.new_group_acc(row1)

        row2 = {"year":2025,"store_id":1,"user_id":1,"cost":15}

        grouper.add_group_acc(acc, row2)
        
        print("ACC IS ", acc)
        self.assertEqual(acc.get("cost",None), 27)
        self.assertEqual(acc.get("purchase_count",None), 2)

        for field in fields_grouped_by:
            self.assertFalse(field in acc)

        res = grouper.expand_with_key(key,acc)            
        expected = {"year":2025,"store_id":1,"user_id":1,"cost":27, "purchase_count":2}
        self.assertEqual(res, expected)
