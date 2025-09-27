import unittest

from selectnode.src.row_filtering import * 
from selectnode.src.row_mapping import * 
from selectnode.src.select_type_config import * 
from selectnode.src.selectnode import * 
from selectnode.src.mocks_middleware import * 


def map_dict_to_vect_cols(cols, row):
    res = []
    for col in cols:
        res.append(str(row[col]))
    return res

def map_vect_to_dict_cols(cols, row):
    res = {}
    for i in range(len(cols)):
        res[cols[i]] = row[i]
    return res


class TestSelectNode(unittest.TestCase):

    def test_query_1_selectnode(self):

        # In order
        in_cols = ["transaction_id", "year", "hour", "sum"]
        out_cols = ["transaction_id", "sum"]
        
        result_grouper = MockMiddleware()
        type_conf = SelectTypeConfiguration(result_grouper, MockMessageBuilder,
            in_fields=in_cols, 
            filters_conf = [
                ["year", EQUALS_ANY, ["2024", "2025"]],
                ["hour", BETWEEN_THAN_OP, [6, 23]],
                ["sum", GREATER_THAN_OP, [75]],
            ],
            out_conf={
                ROW_CONFIG_OUT_COLS: out_cols
            }
        )

        in_middle = MockMiddleware()

        type_map = { "t1": type_conf }

        node = SelectNode(in_middle, type_map)
        node.start()
        
        rows_pass = [
            {"transaction_id": "tr1", 'year': 2024, 'hour': 7, 'sum': 88},
            {"transaction_id": "tr2", 'year': 2025, 'hour': 23, 'sum': 942},
            {"transaction_id": "tr3", 'year': 2024, 'hour': 6, 'sum': 942},
        ]
        #out_cols = ["transaction_id", "sum"]
        expected = [[r["transaction_id"], str(r["sum"])] for r in rows_pass]


        rows_fail = [
            {"transaction_id": "tr4", 'year': 2027, 'hour': 7, 'sum': 88},
            {"transaction_id": "tr5", 'year': 2025, 'hour': 24, 'sum': 942},
            {"transaction_id": "tr6", 'year': 2024, 'hour': 6, 'sum': 55},
        ]

        message = MockMessage("tag1",["query_3323"], ["t1"],
            rows_pass+ rows_fail, lambda r: map_dict_to_vect_cols(in_cols, r)
        )

        in_middle.push_msg(message);

        self.assertEqual(len(result_grouper.msgs), 1)
        self.assertEqual(result_grouper.msgs[0].ind, 0)
        self.assertEqual(result_grouper.msgs[0].msg_from, message)

        got_result = [x for x in result_grouper.msgs[0].payload]
        self.assertEqual(len(got_result),len(rows_pass))

        ind =0
        for elem in expected:
            self.assertEqual(got_result[ind], elem)
            ind+=1


    def test_query_2_selectnode(self):

        # In order
        in_cols = ["product_id", "year", "month", "revenue"]
        out_cols = ["product_id", "month", "revenue"]
        
        result_grouper = MockMiddleware()
        type_conf = SelectTypeConfiguration(result_grouper, MockMessageBuilder,
            in_fields=in_cols, # In order
            filters_conf = [
                ["year", EQUALS_ANY, ["2024", "2025"]]
            ],
            out_conf={
                ROW_CONFIG_ACTIONS: [
                    [MAP_MONTH, {
                        "init_year": 2024,
                        "col_year": "year",
                        "col_month": "month",
                        "col_out": "month"
                    }]
                ],
                ROW_CONFIG_OUT_COLS: out_cols
            })

        in_middle = MockMiddleware()

        type_map = { "t1": type_conf }

        node = SelectNode(in_middle, type_map)
        node.start()
        
        rows_pass = [
            {"product_id": "pr1", 'year': 2024, 'month': 7, 'revenue': 88},
            {"product_id": "pr2", 'year': 2025, 'month': 11, 'revenue': 942},
            {"product_id": "pr3", 'year': 2024, 'month': 6, 'revenue': 942},
        ]
        expected = [
            ["pr1", "7", "88"],
            ["pr2", "23", "942"],
            ["pr3", "6", "942"],
        ]


        rows_fail = [
            {"product_id": "pr4", 'year': 2027, 'month': 7, 'revenue': 88},
            {"product_id": "pr5", 'year': 2022, 'month': 0, 'revenue': 942},
            {"product_id": "pr6", 'year': 2020, 'month': 6, 'revenue': 55},
        ]

        message = MockMessage("tag1",["query_3323"], ["t1"],
            rows_pass+ rows_fail, lambda r: map_dict_to_vect_cols(in_cols, r)
        )

        in_middle.push_msg(message);

        self.assertEqual(len(result_grouper.msgs), 1)
        self.assertEqual(result_grouper.msgs[0].ind, 0)
        self.assertEqual(result_grouper.msgs[0].msg_from, message)

        got_result = [x for x in result_grouper.msgs[0].payload]
        self.assertEqual(len(got_result),len(rows_pass))

        ind =0
        for elem in expected:
            self.assertEqual(got_result[ind], elem)
            ind+=1


    def test_query_3_selectnode(self):

        # In order
        in_cols = ["transaction_id","store_id", "year", "month", "hour", "revenue"]
        out_cols = ["transaction_id", "store_id", "mapped_semester", "revenue"]
        
        result_grouper = MockMiddleware()
        type_conf = SelectTypeConfiguration(result_grouper, MockMessageBuilder,
            in_fields=in_cols, # In order
            filters_conf = [
                ["year", EQUALS_ANY, ["2024", "2025"]],
                ["hour", BETWEEN_THAN_OP, [6, 23]],
            ],
            out_conf={
                ROW_CONFIG_ACTIONS: [
                    [MAP_SEMESTER, {
                        "init_year": 2024,
                        "col_year": "year",
                        "col_month": "month",
                        "col_out": "mapped_semester"
                    }]
                ],
                ROW_CONFIG_OUT_COLS: out_cols
            })

        in_middle = MockMiddleware()

        type_map = { "t1": type_conf }

        node = SelectNode(in_middle, type_map)
        node.start()
        
        #in_cols = ["transaction_id","store_id", "year", "month", "hour", "revenue"]
        rows_pass = [
            {"transaction_id": "tr1", "store_id": "str1", 'revenue': 88,
            'year': 2024, 'month': 1, "hour": 22},
            {"transaction_id": "tr2", "store_id": "str2", 'revenue': 942,
            'year': 2025, 'month': 11, "hour": 11},
            {"transaction_id": "tr3", "store_id": "str3", 'revenue': 942,
            'year': 2024, 'month': 9, "hour": 8},
        ]
        #out_cols = ["transaction_id", "store_id", "mapped_semester", "revenue"]
        expected = [
            ["tr1", "str1","0", "88"], # semester each 3 months? since 2024, 1 month, so 0
            ["tr2", "str2","7", "942"],# since 2024 start, 23 months, 7 semesters, in middle of it?
            ["tr3", "str3","3", "942"],# since 2024 start, 9 months, 3 semesters? 9//3 = 3
        ]


        rows_fail = [
            {"transaction_id": "tr4", "store_id": "str1", 'revenue': 88,
            'year': 2026, 'month': 7, "hour": 22},
            {"transaction_id": "tr5", "store_id": "str2", 'revenue': 942,
            'year': 2025, 'month': 11, "hour": 24},
            {"transaction_id": "tr6", "store_id": "str3", 'revenue': 942,
            'year': 2020, 'month': 6, "hour": 8},
        ]

        message = MockMessage("tag1",["query_3323"], ["t1"],
            rows_pass+ rows_fail, lambda r: map_dict_to_vect_cols(in_cols, r)
        )

        in_middle.push_msg(message);

        self.assertEqual(len(result_grouper.msgs), 1)
        self.assertEqual(result_grouper.msgs[0].ind, 0)
        self.assertEqual(result_grouper.msgs[0].msg_from, message)

        got_result = [x for x in result_grouper.msgs[0].payload]
        self.assertEqual(len(got_result),len(rows_pass))

        ind =0
        for elem in expected:
            self.assertEqual(got_result[ind], elem)
            ind+=1

    def test_query_4_selectnode(self):

        # In order
        in_cols = ["transaction_id","store_id","user_id", "year"]
        out_cols = ["transaction_id","store_id","user_id"]
        
        result_grouper = MockMiddleware()
        type_conf = SelectTypeConfiguration(result_grouper, MockMessageBuilder,
            in_fields=in_cols, # In order
            filters_conf = [
                ["year", EQUALS_ANY, ["2024", "2025"]],
            ],
            out_conf={
                ROW_CONFIG_OUT_COLS: out_cols
            })

        in_middle = MockMiddleware()

        type_map = { "t4": type_conf }

        node = SelectNode(in_middle, type_map)
        node.start()
        
        #in_cols = ["transaction_id","store_id","user_id", "year"]
        rows_pass = [
            {"transaction_id": "tr1", "store_id": "str1", "user_id":"usr1", "year": 2025},
            {"transaction_id": "tr2", "store_id": "str1", "user_id":"usr1", "year": 2024},
            {"transaction_id": "tr3", "store_id": "str3", "user_id":"usr2", "year": 2025},
        ]
        #out_cols = ["transaction_id","store_id","user_id"]
        expected = [
            ["tr1", "str1","usr1"],
            ["tr2", "str1","usr1"],
            ["tr3", "str3","usr2"],
        ]


        rows_fail = [
            {"transaction_id": "tr4", "store_id": "str1", "user_id":"usr1", "year": 2023},
            {"transaction_id": "tr5", "store_id": "str1", "user_id":"usr1", "year": 2022},
            {"transaction_id": "tr6", "store_id": "str3", "user_id":"usr2", "year": 2026},
        ]

        message = MockMessage("tag1",["query_3323"], ["t4"],
            rows_pass+ rows_fail, lambda r: map_dict_to_vect_cols(in_cols, r)
        )

        in_middle.push_msg(message);

        self.assertEqual(len(result_grouper.msgs), 1)
        self.assertEqual(result_grouper.msgs[0].ind, 0)
        self.assertEqual(result_grouper.msgs[0].msg_from, message)

        got_result = [x for x in result_grouper.msgs[0].payload]
        self.assertEqual(len(got_result),len(rows_pass))

        ind =0
        for elem in expected:
            self.assertEqual(got_result[ind], elem)
            ind+=1





    def test_multi_query_1_3_4_together(self):
        # In order.. have to get all columns even if not shared
        in_cols4 = ["transaction_id", "year", "store_id","user_id"]
        in_cols1 = ["transaction_id", "year", "hour", "revenue"]
        in_cols3 = ["transaction_id", "year", "store_id", "month", "hour", "revenue"]

        # All columns together
        in_cols_final = ["transaction_id", "year", "store_id","user_id", "month", "hour", "revenue"]

        out_cols4 = ["transaction_id","store_id","user_id"]
        out_cols1 = ["transaction_id", "revenue"]
        out_cols3 = ["transaction_id", "store_id", "mapped_semester", "revenue"]
        
        result_grouper_4 = MockMiddleware()
        result_grouper_3 = MockMiddleware()
        result_grouper_1 = MockMiddleware()
        type_map = {}

        # Type 4
        type_conf = SelectTypeConfiguration(result_grouper_4, MockMessageBuilder,
            in_fields=in_cols_final, # All cols, drop it after.
            filters_conf = [
                ["year", EQUALS_ANY, ["2024", "2025"]],
            ],
            out_conf={
                ROW_CONFIG_OUT_COLS: out_cols4
            })
        type_map["t4"] = type_conf
        
        ### Type 3
        type_conf = SelectTypeConfiguration(result_grouper_3, MockMessageBuilder,
            in_fields=in_cols_final, # All cols, drop it after.
            filters_conf = [
                ["year", EQUALS_ANY, ["2024", "2025"]],
                ["hour", BETWEEN_THAN_OP, [6, 23]],
            ],
            out_conf={
                ROW_CONFIG_ACTIONS: [
                    [MAP_SEMESTER, {
                        "init_year": 2024,
                        "col_year": "year",
                        "col_month": "month",
                        "col_out": "mapped_semester"
                    }]
                ],
                ROW_CONFIG_OUT_COLS: out_cols3
            })
        type_map["t3"] = type_conf

        ### Type 1
        type_conf = SelectTypeConfiguration(result_grouper_1, MockMessageBuilder,
            in_fields=in_cols_final, # All cols, drop it after.
            filters_conf = [
                ["year", EQUALS_ANY, ["2024", "2025"]],
                ["hour", BETWEEN_THAN_OP, [6, 23]],
                ["revenue", GREATER_THAN_OP, [75]],
            ],
            out_conf={
                ROW_CONFIG_OUT_COLS: out_cols1
            }
        )
        type_map["t1"] = type_conf


        in_middle = MockMiddleware()

        node = SelectNode(in_middle, type_map)
        node.start()
        

        #in_cols_final = ["transaction_id", "year", "store_id","user_id", "month", "hour", "revenue"]
        #in_cols_final = ["month", "hour", "revenue"]
        
        rows = [
            #Filtered by  3 and 1, hour = 24
            {"transaction_id": "tr1", "store_id": "str1", "user_id":"usr1", 
            "year": 2025,"month":5, "hour":24, "revenue":85},
            #Filtered by all year = 2026
            {"transaction_id": "tr2", "store_id": "str1", "user_id":"usr1", 
            "year": 2026,"month":5, "hour":22, "revenue":85}, 

            #Filtered by 1, revenue <= 75
            {"transaction_id": "tr3", "store_id": "str1", "user_id":"usr3", 
            "year": 2025,"month":5, "hour":22, "revenue":75},

            #Filtered by NONE, revenue <= 75
            {"transaction_id": "tr4", "store_id": "str1", "user_id":"usr1", 
            "year": 2025,"month":8, "hour":6, "revenue":90},
        ]

        #out_cols4 = ["transaction_id","store_id","user_id"]
        expected4 = [
            ["tr1","str1","usr1"],
            ["tr3","str1","usr3"],
            ["tr4","str1","usr1"],
        ]
        #out_cols1 = ["transaction_id", "revenue"]
        expected1 = [
            ["tr4","90"],
        ]
        
        #out_cols3 = ["transaction_id", "store_id", "mapped_semester", "revenue"]
        expected3 = [
            ["tr3", "str1","5","75"], #(12+5)//3 == 5
            ["tr4", "str1","6","90"],#(12+8)//3 == 6
        ]

        message = MockMessage("tag1",["q4","q3","q1"], ["t4","t3","t1"],
            rows, lambda r: map_dict_to_vect_cols(in_cols_final, r)
        )

        in_middle.push_msg(message);

        got_result4 = [x for x in result_grouper_4.msgs[0].payload]
        got_result3 = [x for x in result_grouper_3.msgs[0].payload]
        got_result1 = [x for x in result_grouper_1.msgs[0].payload]
        
        self.assertEqual(len(got_result4),len(expected4))
        self.assertEqual(len(got_result3),len(expected3))
        self.assertEqual(len(got_result1),len(expected1))

        ind =0
        for elem in expected3:
            self.assertEqual(got_result3[ind], elem)
            ind+=1
        
        ind =0
        for elem in expected4:
            self.assertEqual(got_result4[ind], elem)
            ind+=1

        ind =0
        for elem in expected1:
            self.assertEqual(got_result1[ind], elem)
            ind+=1





"""
    def test_multiquery_selectnode(self):
        in_fields = ["year", "hour", "sum"]
        filters_serial = [
                ["year", EQUALS_ANY, [2024, 2025]],
                ["hour", BETWEEN_THAN_OP, [6, 23]],
                ["sum", GREATER_THAN_OP, [75]],
        ]

        filters_serial2 = [
                ["year", EQUALS_ANY, [2024, 2025]],
                ["hour", BETWEEN_THAN_OP, [6, 23]],
        ]

        result_grouper = MockMiddleware()
        in_middle = MockMiddleware()

        type_map = {
            "query_t_1": SelectTypeConfiguration(result_grouper, MockMessageBuilder, in_fields =in_fields,filters_conf = filters_serial),
            "query_t_2": SelectTypeConfiguration(result_grouper, MockMessageBuilder, in_fields =in_fields,filters_conf = filters_serial2)
        }

        node = SelectNode(in_middle, type_map)
        node.start()

        rows_pass = [
            {'year': 2024, 'hour': 7, 'sum': 88},
            {'year': 2025, 'hour': 23, 'sum': 942},
            {'year': 2024, 'hour': 6, 'sum': 942},
        ]
        rows_fail = [
            {'year': 2027, 'hour': 7, 'sum': 88},
            {'year': 2025, 'hour': 24, 'sum': 942},
            {'year': 2024, 'hour': 6, 'sum': 55},
        ]

        message = MockMessage("tag1",["query_3323", "query_3342"], ["query_t_1", "query_t_2"],
            rows_pass+ rows_fail, map_dict_to_vect
        )

        in_middle.push_msg(message);
        self.assertTrue(len(result_grouper.msgs)==2)

        # CHECK STILL QUERY 1 is solved
        self.assertTrue(result_grouper.msgs[0].ind == 0)
        self.assertTrue(result_grouper.msgs[0].msg_from == message)

        got_result = [map_vect_to_dict(el) for el in result_grouper.msgs[0].payload]

        for elem in rows_pass:
            self.assertTrue(elem in got_result)

        self.assertTrue(len(got_result) == len(rows_pass))


        # CHECK QUERY 2
        self.assertTrue(result_grouper.msgs[1].ind == 1)
        self.assertTrue(result_grouper.msgs[1].msg_from == message)
        got_result = [map_vect_to_dict(el) for el in result_grouper.msgs[1].payload]

        rows_pass2 = [
            {'year': 2024, 'hour': 7, 'sum': 88},
            {'year': 2025, 'hour': 23, 'sum': 942},
            {'year': 2024, 'hour': 6, 'sum': 942},
            {'year': 2024, 'hour': 6, 'sum': 55},
        ]
        for elem in rows_pass2:
            self.assertTrue(elem in got_result)

        self.assertTrue(len(got_result) == len(rows_pass2))
"""