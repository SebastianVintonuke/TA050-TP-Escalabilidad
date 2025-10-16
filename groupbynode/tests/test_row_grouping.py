import unittest
from groupbynode.src.row_grouping import *
import time
class TestKeepAllRowsAction(unittest.TestCase):
    def test_all_rows_kept(self):
        action = KeepAllRowsAction()
        acc = []
        rows = [{'score': i} for i in range(5)]
        for r in rows:
            action.add_to(acc, r)
        self.assertEqual(len(acc), 5)
        self.assertEqual(acc, rows)

class TestKeepTopAction(unittest.TestCase):
    def test_keep_top_dict(self):
        action = KeepTopAction(comp_key='score')
        acc = []
        action.add_new(acc, {'name': 'A', 'score': 10})
        action.add_to(acc, {'name': 'B', 'score': 15})  # Should replace
        action.add_to(acc, {'name': 'C', 'score': 5})   # Should not replace
        self.assertEqual(len(acc), 1)
        self.assertEqual(acc[0]['score'], 15)

    def test_keep_top_list(self):
        action = KeepTopAction(comp_key=1)
        acc = []
        action.add_new(acc, ['A', 10])
        action.add_to(acc, ['B', 15])
        action.add_to(acc, ['C', 5])
        self.assertEqual(acc[0][1], 15)


    def test_keep_top_str(self):
        action = KeepTopAction(comp_key=1)
        acc = []
        action.add_new(acc, ['A', "99.0"])
        action.add_to(acc, ['B', "100.0"])
        action.add_to(acc, ['C', "5.0"])
        self.assertEqual(acc[0][1], 100.0)


class TestKeepTopKAction(unittest.TestCase):
    def test_top_k_basic(self):
        action = KeepTopKAction(comp_key='score', limit=3)
        acc = []
        for score in [10, 20, 30, 25, 5]:
            action.add_to(acc, {'score': score})
        scores = [row['score'] for row in acc]
        self.assertEqual(scores, [30, 25, 20])

    def test_keep_top_k_str(self):
        action = KeepTopKAction(comp_key=1, limit=3)
        acc = []
        action.add_new(acc, ['A', "99.0"])
        action.add_to(acc, ['B', "100.0"])
        action.add_to(acc, ['C', "5.0"])
        self.assertEqual(acc[0][1], 100.0)
        self.assertEqual(acc[1][1], 99.0)


    def test_top_k_limit_1(self):
        action = KeepTopKAction(comp_key='score', limit=1)
        acc = []
        for score in [5, 50, 10]:
            action.add_to(acc, {'score': score})
        self.assertEqual(acc[0]['score'], 50)


    def test_top_k_limit_4(self):
        action = KeepTopKAction(comp_key='score', limit=4)
        acc = []
        for score in [5, 50, 10, 30,10,40]:
            action.add_to(acc, {'score': score})
        self.assertEqual(acc[0]['score'], 50)
        self.assertEqual(acc[1]['score'], 40)
        self.assertEqual(acc[2]['score'], 30)

    def test_top_k_duplicate_values(self):
        action = KeepTopKAction(comp_key='score', limit=2)
        acc = []
        action.add_to(acc, {'score': 10})
        action.add_to(acc, {'score': 10})
        action.add_to(acc, {'score': 5})
        self.assertEqual(len(acc), 2)
        self.assertTrue(all(r['score'] == 10 for r in acc))

    def test_topk_ascdesc(self):
        action = KeepAscOrDescK(comp_key = "score",comp_key2="age", limit= 3)
        rows = [
            {'score': 10, "age": 5},
            {'score': 10, "age": 10},
            {'score': 10, "age": 20},
            {'score': 10, "age": 10},
            {'score': 10, "age": 9},
            {'score': 10, "age": 10},
            {'score': 10, "age": 10},
            {'score': 11, "age":40}
        ]
        for ind,row in enumerate(rows):
            row["ind"] = ind

        acc = []
        for r in rows:
            action.add_to(acc, r)

        expected = [
            rows[-1],
            rows[0],
            rows[4],
        ]
        self.assertEqual(len(acc), 3)
        for ind, exp in enumerate(expected):
            self.assertEqual(acc[ind], exp)

"""
    def test_bench_topk_ascdesc(self):
        action = KeepAscOrDescK(comp_key = "score",comp_key2="age", limit= 3)
        rows = [
            {'score': 10, "age": 5},
            {'score': 10, "age": 11},
            {'score': 10, "age": 20},
            {'score': 10, "age": 12},
            {'score': 10, "age": 9},
            {'score': 10, "age": 13},
            {'score': 10, "age": 14},
            {'score': 11, "age":40}
        ] * 10000
        ITERS = 5

        init_time = round(time.time()*1000)

        for i in range(ITERS):
            acc = []
            for r in rows:
                action.add_to(acc, r)


            expected = [
                rows[-1],
                rows[-1],
                rows[-1],
            ]
        
            self.assertEqual(len(acc), 3)
            for ind, exp in enumerate(expected):
                self.assertEqual(acc[ind], exp)

        end_time = round(time.time()*1000)
        print(f"KeepASCKORDESC TOOK, {end_time-init_time}ms")

        action = KeepTopKAction(comp_key='score', limit=3)
        init_time = round(time.time()*1000)

        for i in range(ITERS):
            acc = []
            for r in rows:
                action.add_to(acc, r)

            expected = [
                rows[-1],
                rows[-1],
                rows[-1],
            ]
            self.assertEqual(len(acc), 3)
            for ind, exp in enumerate(expected):
                self.assertEqual(acc[ind], exp)
        
        end_time = round(time.time()*1000)

        print(f"KeepTopK TOOK, {end_time-init_time}ms")

        #action = KeepKHeap(comp_key = "score",comp_key2="age", limit= 3)
        action = KeepAscOrDescK(comp_key = "score",comp_key2="age", limit= 3)
        
        init_time = round(time.time()*1000)

        for i in range(ITERS):
            acc = []
            action.add_all(acc, rows)
            #for r in rows:
            #    action.add_to(acc, r)
            #acc = action.finished(acc)
            #acc = maintain_top_k(rows, 3, "score", "age")


            expected = [
                rows[-1],
                rows[-1],
                rows[-1],
            ]
            self.assertEqual(len(acc), 3)
            for ind, exp in enumerate(expected):
                self.assertEqual(acc[ind], exp)
        
        end_time = round(time.time()*1000)
        print(f"KeepTopK HEAP TOOK, {end_time-init_time}ms")
"""

class TestKeepLeastKAction(unittest.TestCase):
    def test_least_k_basic(self):
        action = KeepLeastKAction(comp_key='score', limit=3)
        acc = []
        for score in [10, 5, 15, 2, 8]:
            action.add_to(acc, {'score': score})
        scores = [r['score'] for r in acc]
        self.assertEqual(scores, [2, 5, 8])

    def test_least_k_list_rows(self):
        action = KeepLeastKAction(comp_key=1, limit=2)
        acc = []
        rows = [['a', 10], ['b', 5], ['c', 15]]
        for row in rows:
            action.add_to(acc, row)
        self.assertEqual([r[1] for r in acc], [5, 10])

    def test_least_k_equal_values(self):
        action = KeepLeastKAction(comp_key='score', limit=2)
        acc = []
        for _ in range(5):
            action.add_to(acc, {'score': 10})
        self.assertEqual(len(acc), 2)
        self.assertTrue(all(r['score'] == 10 for r in acc))

class TestRowGrouper(unittest.TestCase):
    def test_grouper_keep_all(self):
        config = [KEEP_ALL_ROWS, {}]
        grouper = RowGrouper(config)
        acc = grouper.new_group_acc({'score': 1})
        for i in range(2, 5):
            grouper.add_group_acc(acc, {'score': i})
        self.assertEqual(len(acc), 4)

    def test_grouper_top_k(self):
        config = [KEEP_TOP_K, {'comp_key': 'revenue', 'limit': 3}]
        grouper = RowGrouper(config)
        acc = grouper.new_group_acc({"id":1,'revenue': 10})
        grouper.add_group_acc(acc, {"id":2,'revenue': 50})
        grouper.add_group_acc(acc, {"id":3,'revenue': 20})
        grouper.add_group_acc(acc, {"id":4,'revenue': 5})
        grouper.add_group_acc(acc, {"id":5,'revenue': 20})
        grouper.add_group_acc(acc, {"id":6,'revenue': 20})

        self.assertEqual([r['revenue'] for r in acc], [50, 20 , 20])

    def test_grouper_top_k_keeps_order(self):
        config = [KEEP_TOP_K, {'comp_key': 'revenue', 'limit': 3}]
        grouper = RowGrouper(config)
        acc = grouper.new_group_acc({"id":1,'revenue': 10})
        grouper.add_group_acc(acc, {"id":2,'revenue': 50})
        grouper.add_group_acc(acc, {"id":3,'revenue': 20})
        grouper.add_group_acc(acc, {"id":4,'revenue': 5})
        grouper.add_group_acc(acc, {"id":5,'revenue': 20})
        grouper.add_group_acc(acc, {"id":6,'revenue': 20})

        self.assertEqual([r['revenue'] for r in acc], [50, 20 , 20])
        self.assertEqual([r['id'] for r in acc], [2, 3 , 5])

    def test_grouper_top_k_keeps_order(self):
        config = [KEEP_TOP_K, {'comp_key': 1, 'limit': 3}]
        grouper = RowGrouper(config)
        acc = grouper.new_group_acc([1, 10])
        grouper.add_group_acc(acc, [2, 50])
        grouper.add_group_acc(acc, [3, 20])
        grouper.add_group_acc(acc, [4, 5])
        grouper.add_group_acc(acc, [5, 20])
        grouper.add_group_acc(acc, [6, 20])

        self.assertEqual([r[1] for r in acc], [50, 20 , 20])

    def test_grouper_top_k_keeps_order_vector(self):
        config = [KEEP_TOP_K, {'comp_key': 1, 'limit': 3}]
        grouper = RowGrouper(config)
        acc = grouper.new_group_acc([1, 10])
        grouper.add_group_acc(acc, [2, 50])
        grouper.add_group_acc(acc, [3, 20])
        grouper.add_group_acc(acc, [4, 5])
        grouper.add_group_acc(acc, [5, 20])
        grouper.add_group_acc(acc, [6, 20])

        self.assertEqual([r[1] for r in acc], [50, 20 , 20])
        self.assertEqual([r[0] for r in acc], [2, 3 , 5])


    def test_grouper_invalid_config(self):
        with self.assertRaises(AssertionError):
            RowGrouper(['invalid_action', {}])

