import unittest
from groupbynode.src.row_grouping import * 

class TestRowGrouper(unittest.TestCase):


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
