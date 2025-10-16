import unittest

from middleware.mocks.middleware import *

from common.config.row_filtering import *
from common.config.row_mapping import *
from common.config.type_expander import *
from selectnode.src.select_type_config import *
from selectnode.src.selectnode import *
from selectnode.src.config_init import *

from groupbynode.src.groupbynode import *
from groupbynode.src.groupby_initialize import *
from groupbynode.src.topk_initialize import *

from groupbynode.src.topk_initialize import *

from middleware.memory_middleware import * 
from middleware.routing.csv_message import * 
from middleware.routing.query_types import * 


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



class NodesSetup:
    def mock_setup():
        return NodesSetup(
            msg_type= CSVMessage,# Initial msg_type
            select_middleware = SerializeMemoryMiddleware(),
            join_middleware = SerializeMemoryMiddleware(),
            groupby_middleware = SerializeMemoryMiddleware(),
            topk_middleware = SerializeMemoryMiddleware(),
            result_middleware = SerializeMemoryMiddleware(),
        )


    def real_setup():
        return NodesSetup(
            msg_type= CSVMessage,# Initial msg_type
            select_middleware = SelectTasksMiddleware(),
            join_middleware = JoinTasksMiddleware(1, ind = 0),
            groupby_middleware = GroupbyTasksMiddleware(1, ind = 0),
            topk_middleware = SerializeMemoryMiddleware(),
            result_middleware = SerializeMemoryMiddleware(),
        )



    def __init__(self, msg_type, select_middleware, join_middleware, groupby_middleware, topk_middleware, result_middleware):
        self.select_middleware=IntermediateMiddleware(select_middleware)
        self.join_middleware=IntermediateMiddleware(join_middleware)
        self.groupby_middleware=IntermediateMiddleware(groupby_middleware)
        self.topk_middleware=IntermediateMiddleware(topk_middleware)
        self.result_middleware=IntermediateMiddleware(result_middleware)
        self.msg_type = msg_type

    def get_select_node(self, msg_type = None):
        types_expander = TypeExpander()
        add_selectnode_config(types_expander, self.result_middleware, self.groupby_middleware)
        if msg_type:
            return SelectNode(self.select_middleware, msg_type, types_expander)

        return SelectNode(self.select_middleware, self.msg_type, types_expander)

    def get_join_node(self, msg_type = None):
        types_expander = TypeExpander()
        add_joinnode_config(types_expander, self.result_middleware, self.join_middleware)

        if msg_type:
            return JoinNode(self.join_middleware, msg_type, types_expander)
            
        return JoinNode(self.join_middleware, self.msg_type, types_expander)


    def get_topk_node(self, msg_type = None):
        config = configure_types_topk(self.join_middleware)
        if msg_type:
            return GroupbyNode(self.topk_middleware, msg_type, config)
            
        return GroupbyNode(self.topk_middleware, self.msg_type, config)

    def get_groupby_node(self, msg_type = None):
        config = configure_types_groupby(
                self.join_middleware, self.topk_middleware) #topk_middleware_type = HashedMemoryMessageBuilder

        if msg_type:
            return GroupbyNode(self.groupby_middleware, msg_type, config)
            
        return GroupbyNode(self.groupby_middleware, self.msg_type, config)


def get_row_query_transactions(dict_data):
    return map_dict_to_vect_cols(SELECT_TRANSACTION_SHARED_IN_FIELDS, dict_data)

def parse_output_rows_query_1(rows):
    return [[row["transaction_id"], row["revenue"]] for row in rows]


def get_row_query_transaction_items(dict_data):
    return map_dict_to_vect_cols(SELECT_TRANSACTION_ITEMS_IN_FIELDS, dict_data)

class TestEOFProtocol(unittest.TestCase):
    def test_compiled_config(self):
        types_expander = TypeExpander()
        result_grouper = MockMiddleware()
        groupby_middleware = MockMiddleware()
        add_selectnode_config(types_expander, result_grouper, groupby_middleware)
        self.assertFalse(False) # Just be able to compile it basically.

    def test_query_1_simple_set_up(self):

        #MemoryMessage , CSVMessage
        #nested_joins_middleware = SerializeMemoryMiddleware() # Serialize message since it will be the same node that receives the action.

        ## INIT NODES
        nodes_setup = NodesSetup.mock_setup()

        select_node = nodes_setup.get_select_node()
        select_node.start()

        results = nodes_setup.result_middleware

        msg = CSVMessageBuilder.with_credentials(["q_id"], [QUERY_1])

        rows = [
            {
                "transaction_id": "tr_1",
                "year": "2023",
                "store_id": "st_1",
                "user_id": "u_1",
                "month": "1",
                "hour": "13",
                "revenue": "10",
            },
            {
                "transaction_id": "tr_1",
                "year": "2024",
                "store_id": "st_1",
                "user_id": "u_1",
                "month": "1",
                "hour": "13",
                "revenue": "85",
            }
        ]

        exp_rows_q1 = parse_output_rows_query_1(rows[1:])

        for row in rows:
            msg.add_row(get_row_query_transactions(row))

        msg_eof = msg.clone()
        msg_eof.set_as_eof(1)

        nodes_setup.select_middleware.push_msg(msg)
        nodes_setup.select_middleware.push_msg(msg_eof)
        self.assertEqual(len(results.msgs), 2)

        res = CSVMessage(results.msgs[0].serialize_payload())
        q1_res_rows = [row for row in res.stream_rows()]
        self.assertEqual(len(q1_res_rows) , len(exp_rows_q1))
        for i in range(len(exp_rows_q1)):
            self.assertEqual(q1_res_rows[i] , exp_rows_q1[i])


        headers_msg = results.msgs[1].headers
        self.assertTrue(headers_msg.is_eof());
        self.assertEqual(headers_msg.msg_count, 1);




    def test_query_1_selectnode(self):

        # In order
        in_cols = ["transaction_id", "year", "hour", "sum"]
        out_cols = ["transaction_id", "sum"]

        result_grouper = MockMiddleware()
        type_conf = SelectTypeConfiguration(
            result_grouper,
            BareMockMessageBuilder,
            in_fields=in_cols,
            filters_conf=[
                ["year", EQUALS_ANY, ["2024", "2025"]],
                ["hour", BETWEEN_THAN_OP, [6, 23]],
                ["sum", GREATER_THAN_OP, [75]],
            ],
            out_conf={ROW_CONFIG_OUT_COLS: out_cols},
        )

        in_middle = MockMiddleware()

        type_exp = TypeExpander()
        type_exp.add_configurations("t1", type_conf)

        node = SelectNode(in_middle, MockMessage, type_exp)
        node.start_single()

        rows_pass = [
            {"transaction_id": "tr1", "year": 2024, "hour": 7, "sum": 88},
            {"transaction_id": "tr2", "year": 2025, "hour": 23, "sum": 942},
            {"transaction_id": "tr3", "year": 2024, "hour": 6, "sum": 942},
        ]
        # out_cols = ["transaction_id", "sum"]
        expected = [[r["transaction_id"], str(r["sum"])] for r in rows_pass]

        rows_fail = [
            {"transaction_id": "tr4", "year": 2027, "hour": 7, "sum": 88},
            {"transaction_id": "tr5", "year": 2025, "hour": 24, "sum": 942},
            {"transaction_id": "tr6", "year": 2024, "hour": 6, "sum": 55},
        ]

        message = BareMockMessageBuilder.for_payload(
            ["query_3323"],
            ["t1"],
            rows_pass + rows_fail,
            lambda r: map_dict_to_vect_cols(in_cols, r),
        )

        in_middle.push_msg(message)

        self.assertEqual(len(result_grouper.msgs), message.headers.len_queries())

        for ind, exp_out_headers in enumerate(message.headers.split()):            
            self.assertEqual(
                result_grouper.msgs[ind].headers.to_dict(), 
                exp_out_headers.to_dict())

        got_result = [x for x in result_grouper.msgs[0].payload]
        self.assertEqual(len(got_result), len(rows_pass))

        ind = 0
        for elem in expected:
            self.assertEqual(got_result[ind], elem)
            ind += 1
