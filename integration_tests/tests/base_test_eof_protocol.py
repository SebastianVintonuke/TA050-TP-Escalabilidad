import unittest

from common.config.row_filtering import *
from common.config.row_mapping import *
from common.config.type_expander import *

from integration_tests.src.nodes_setup import NodesSetup

#from middleware.memory_middleware import * 
from middleware.routing.csv_message import * 
from middleware.mocks.middleware import *

from middleware.routing.query_types import * 

from selectnode.src.config_init import SELECT_TRANSACTION_ITEMS_IN_FIELDS, SELECT_TRANSACTION_SHARED_IN_FIELDS


from abc import ABC, abstractmethod
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

def get_row_query_transactions(dict_data):
    return map_dict_to_vect_cols(SELECT_TRANSACTION_SHARED_IN_FIELDS, dict_data)

def parse_output_rows_query_1(rows):
    return [[row["transaction_id"], row["revenue"]] for row in rows]


def get_row_query_transaction_items(dict_data):
    return map_dict_to_vect_cols(SELECT_TRANSACTION_ITEMS_IN_FIELDS, dict_data)

class BaseEOFProtocolTest(ABC):
    def wrap_intermediate(msg_type, select_middleware, join_middleware, groupby_middleware, topk_middleware, result_middleware):
        return NodesSetup(
            msg_type= msg_type,# Initial msg_type
            select_middleware = IntermediateMiddleware(select_middleware),
            join_middleware = IntermediateMiddleware(join_middleware),
            groupby_middleware = IntermediateMiddleware(groupby_middleware),
            topk_middleware = IntermediateMiddleware(topk_middleware),
            result_middleware = IntermediateMiddleware(result_middleware),
        )



    @abstractmethod
    def get_node_setup(self):
        """Subclasses must implement this method to return NodeSetup instance."""
        return None

    def test_query_1_simple_set_up(self):

        #MemoryMessage , CSVMessage
        #nested_joins_middleware = SerializeMemoryMiddleware() # Serialize message since it will be the same node that receives the action.

        ## INIT NODES
        nodes_setup = self.get_node_setup()

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


    def test_query_1_eof_is_inverted_reaches_inverted_to_result_node(self):
        #MemoryMessage , CSVMessage
        #nested_joins_middleware = SerializeMemoryMiddleware() # Serialize message since it will be the same node that receives the action.

        ## INIT NODES
        nodes_setup = self.get_node_setup()

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

        nodes_setup.select_middleware.push_msg(msg_eof)
        nodes_setup.select_middleware.push_msg(msg)

        self.assertEqual(len(results.msgs), 2)

        res = CSVMessage(results.msgs[1].serialize_payload())
        q1_res_rows = [row for row in res.stream_rows()]
        self.assertEqual(len(q1_res_rows) , len(exp_rows_q1))
        for i in range(len(exp_rows_q1)):
            self.assertEqual(q1_res_rows[i] , exp_rows_q1[i])


        headers_msg = results.msgs[0].headers
        self.assertTrue(headers_msg.is_eof());
        self.assertEqual(headers_msg.msg_count, 1);




    def test_query_1_eof_is_inverted_reaches_inverted_to_result_node_with_full_filtered_msg(self):
        #MemoryMessage , CSVMessage
        #nested_joins_middleware = SerializeMemoryMiddleware() # Serialize message since it will be the same node that receives the action.

        ## INIT NODES
        nodes_setup = self.get_node_setup()

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

        msg.add_row(get_row_query_transactions(rows[0]))

        msg_eof = msg.clone()
        msg_eof.set_as_eof(2)

        msg2 = msg.clone()
        msg2.add_row(get_row_query_transactions(rows[1]))

        nodes_setup.select_middleware.push_msg(msg_eof)
        nodes_setup.select_middleware.push_msg(msg) # Filtered completely by select
        nodes_setup.select_middleware.push_msg(msg2)
        self.assertEqual(len(results.msgs), 3);

        # First message fully filtered is not eof.
        self.assertFalse(results.msgs[1].is_eof());

        # But has len 0
        res = CSVMessage(results.msgs[1].serialize_payload())
        q1_res_rows = [row for row in res.stream_rows()]
        self.assertEqual(len(q1_res_rows), 0)

        res = CSVMessage(results.msgs[2].serialize_payload())
        q1_res_rows = [row for row in res.stream_rows()]
        self.assertEqual(len(q1_res_rows) , len(exp_rows_q1))
        for i in range(len(exp_rows_q1)):
            self.assertEqual(q1_res_rows[i] , exp_rows_q1[i])


        headers_msg = results.msgs[0].headers
        self.assertTrue(headers_msg.is_eof());
        self.assertEqual(headers_msg.msg_count, 2);



    def test_query_2_eof_is_inverted_reaches_inverted_to_groupby_but_it_waits(self):
        #MemoryMessage , CSVMessage
        #nested_joins_middleware = SerializeMemoryMiddleware() # Serialize message since it will be the same node that receives the action.

        ## INIT NODES
        nodes_setup = self.get_node_setup()

        select_node = nodes_setup.get_select_node()
        select_node.start()

        groupby_node = nodes_setup.get_groupby_node()
        groupby_node.start()


        groupby_middle = nodes_setup.groupby_middleware

        msg = CSVMessageBuilder.with_credentials(["q_id"], [QUERY_2])

        rows = [
            {
                "product_id": "pr_1",
                "year": "2023",
                "month": "1",
                "revenue": "100",
                "quantity": "2",
            },
            {
                "product_id": "pr_1",
                "year": "2024",
                "month": "1",
                "revenue": "100",
                "quantity": "2",
            },
            {
                "product_id": "pr_2",
                "year": "2024",
                "month": "1",
                "revenue": "5",
                "quantity": "2",
            },
            {
                "product_id": "pr_2",
                "year": "2024",
                "month": "1",
                "revenue": "5",
                "quantity": "1",
            },
        ]


        for row in rows:
            msg.add_row(get_row_query_transaction_items(row))

        msg_eof = msg.clone()
        msg_eof.set_as_eof(1)

        nodes_setup.select_middleware.push_msg(msg_eof)

        self.assertEqual(len(groupby_middle.msgs), 1)

        # No message sent, eof not reached 1 message left to be sent.
        self.assertEqual(len(nodes_setup.topk_middleware.msgs), 0)
        self.assertEqual(len(nodes_setup.result_middleware.msgs), 0)
        self.assertEqual(len(nodes_setup.join_middleware.msgs), 0)

        nodes_setup.select_middleware.push_msg(msg)

        self.assertEqual(len(groupby_middle.msgs), 2)

        # Grouped message was sent to topk together with EOF
        self.assertEqual(len(nodes_setup.topk_middleware.msgs), 2)
        self.assertEqual(len(nodes_setup.result_middleware.msgs), 0)
        self.assertEqual(len(nodes_setup.join_middleware.msgs), 0)


        exp_rows_q2_groupby = [
            #["product_id", "month", "revenue", "quantity"]
            ["pr_1", "1", "100.0", "2.0"], # Quantity is float for now
            ["pr_2", "1", "10.0", "3.0"],
        ]


        self.assertFalse(nodes_setup.topk_middleware.msgs[0].headers.is_eof());
        res = CSVMessage(nodes_setup.topk_middleware.msgs[0].serialize_payload())
        q2_res_rows = [row for row in res.stream_rows()]
        self.assertEqual(len(q2_res_rows) , len(exp_rows_q2_groupby))
        for i in range(len(exp_rows_q2_groupby)):
            self.assertEqual(q2_res_rows[i] , exp_rows_q2_groupby[i])

        headers_msg = nodes_setup.topk_middleware.msgs[1].headers
        self.assertTrue(headers_msg.is_eof());
        self.assertEqual(headers_msg.msg_count, 1);




    def test_query_2_eof_is_inverted_reaches_groupby_and_topk_outputs_res(self):
        #MemoryMessage , CSVMessage
        #nested_joins_middleware = SerializeMemoryMiddleware() # Serialize message since it will be the same node that receives the action.

        ## INIT NODES
        nodes_setup = self.get_node_setup()

        select_node = nodes_setup.get_select_node()
        select_node.start()

        groupby_node = nodes_setup.get_groupby_node()
        groupby_node.start()

        topk_node = nodes_setup.get_topk_node()
        topk_node.start()


        groupby_middle = nodes_setup.groupby_middleware

        msg = CSVMessageBuilder.with_credentials(["q_id"], [QUERY_2])

        rows = [
            {
                "product_id": "pr_1",
                "year": "2023",
                "month": "1",
                "revenue": "100",
                "quantity": "2",
            },
            {
                "product_id": "pr_1",
                "year": "2024",
                "month": "1",
                "revenue": "100",
                "quantity": "2",
            },
            {
                "product_id": "pr_2",
                "year": "2024",
                "month": "1",
                "revenue": "5",
                "quantity": "2",
            },
            {
                "product_id": "pr_2",
                "year": "2024",
                "month": "1",
                "revenue": "5",
                "quantity": "2",
            },
            {
                "product_id": "pr_top_rev",
                "year": "2024",
                "month": "1",
                "revenue": "99",
                "quantity": "1",
            },
            {
                "product_id": "pr_top_q",
                "year": "2024",
                "month": "1",
                "revenue": "3",
                "quantity": "5",
            },
        ]

        # Add filtered rows by topk
        for i in range(1, 10):
            rows.append({
                "product_id": f"pr_not_top_{i}",
                "year": "2024",
                "month": "1",
                "revenue": "3",
                "quantity": "1",
            })


        for row in rows:
            msg.add_row(get_row_query_transaction_items(row))

        msg_eof = msg.clone()
        msg_eof.set_as_eof(1)

        nodes_setup.select_middleware.push_msg(msg_eof)

        self.assertEqual(len(groupby_middle.msgs), 1)

        # No message sent, eof not reached 1 message left to be sent.
        self.assertEqual(len(nodes_setup.topk_middleware.msgs), 0)
        self.assertEqual(len(nodes_setup.result_middleware.msgs), 0)
        self.assertEqual(len(nodes_setup.join_middleware.msgs), 0)

        nodes_setup.select_middleware.push_msg(msg)

        self.assertEqual(len(groupby_middle.msgs), 2)

        # Grouped message was sent to topk together with EOF
        self.assertEqual(len(nodes_setup.topk_middleware.msgs), 2)
        self.assertEqual(len(nodes_setup.result_middleware.msgs), 0)

        # Topk also sent message and EOF, its divided in two types so 2 message per type == 4 messages
        self.assertEqual(len(nodes_setup.join_middleware.msgs), 4)


        # We know the order is this for now
        msg_out_rev = nodes_setup.join_middleware.msgs[0] # Since its sent on EOF then its sent on blocks 0 and 1 are for rev
        msg_out_qua = nodes_setup.join_middleware.msgs[2] 

        self.assertFalse(msg_out_rev.headers.is_eof());
        self.assertFalse(msg_out_qua.headers.is_eof());

        if msg_out_rev.headers.types != [QUERY_2_REVENUE]:
            # Just in case if not revenue then invert it
            tmp = msg_out_rev
            msg_out_rev = msg_out_qua
            msg_out_qua = tmp

        # Check TOP K for revenue
        exp_rows_q2_topk_rev = [
            #["product_id", "month", "revenue"]
            ["pr_1", "1", "100.0"], # Just top one
            #["pr_top_rev", "1", "99.0"], 
            #["pr_2", "1", "10.0"],
        ]

        res = CSVMessage(msg_out_rev.serialize_payload())
        res_rows = [row for row in res.stream_rows()]

        self.assertEqual(len(res_rows) , len(exp_rows_q2_topk_rev))
        for i in range(len(exp_rows_q2_topk_rev)):
            self.assertEqual(res_rows[i] , exp_rows_q2_topk_rev[i])


        # Check TOP K for quantity
        exp_rows_q2_topk_quantity = [
            #["product_id", "month", "quantity"]
            ["pr_top_q", "1", "5.0"],
            #["pr_2", "1", "4.0"],
            #["pr_1", "1", "100.0", "2.0"],
        ]

        res = CSVMessage(msg_out_qua.serialize_payload())
        res_rows = [row for row in res.stream_rows()]

        self.assertEqual(len(res_rows) , len(exp_rows_q2_topk_quantity))
        for i in range(len(exp_rows_q2_topk_quantity)):
            self.assertEqual(res_rows[i] , exp_rows_q2_topk_quantity[i])


        # Check EOFS
        headers_msg_1 = nodes_setup.join_middleware.msgs[1].headers
        headers_msg_2 = nodes_setup.join_middleware.msgs[3].headers
        
        self.assertTrue(headers_msg_1.is_eof());
        self.assertEqual(headers_msg_1.msg_count, 1);
        
        self.assertTrue(headers_msg_2.is_eof());
        self.assertEqual(headers_msg_2.msg_count, 1);

        if headers_msg_1.types == [QUERY_2_REVENUE]:
            self.assertEqual(headers_msg_1.types, [QUERY_2_REVENUE])
            self.assertEqual(headers_msg_2.types, [QUERY_2_QUANTITY])
        else:
            self.assertEqual(headers_msg_2.types, [QUERY_2_REVENUE])
            self.assertEqual(headers_msg_1.types, [QUERY_2_QUANTITY])





    def test_query_2_eof_is_inverted_reaches_join_but_it_waits_both_sides(self):
        #MemoryMessage , CSVMessage
        #nested_joins_middleware = SerializeMemoryMiddleware() # Serialize message since it will be the same node that receives the action.

        ## INIT NODES
        nodes_setup = self.get_node_setup()

        select_node = nodes_setup.get_select_node()
        select_node.start()

        groupby_node = nodes_setup.get_groupby_node()
        groupby_node.start()

        topk_node = nodes_setup.get_topk_node()
        topk_node.start()

        join_node = nodes_setup.get_join_node()
        join_node.start()


        groupby_middle = nodes_setup.groupby_middleware

        msg = CSVMessageBuilder.with_credentials(["q_id"], [QUERY_2])

        rows = [
            {
                "product_id": "pr_1",
                "year": "2023",
                "month": "1",
                "revenue": "100",
                "quantity": "2",
            },
            {
                "product_id": "pr_1",
                "year": "2024",
                "month": "1",
                "revenue": "100",
                "quantity": "2",
            },
            {
                "product_id": "pr_2",
                "year": "2024",
                "month": "1",
                "revenue": "5",
                "quantity": "2",
            },
            {
                "product_id": "pr_2",
                "year": "2024",
                "month": "1",
                "revenue": "5",
                "quantity": "2",
            },
            {
                "product_id": "pr_top_rev",
                "year": "2024",
                "month": "1",
                "revenue": "99",
                "quantity": "1",
            },
            {
                "product_id": "pr_top_q",
                "year": "2024",
                "month": "1",
                "revenue": "3",
                "quantity": "5",
            },
        ]

        # Add filtered rows by topk
        for i in range(1, 10):
            rows.append({
                "product_id": f"pr_not_top_{i}",
                "year": "2024",
                "month": "1",
                "revenue": "3",
                "quantity": "1",
            })


        for row in rows:
            msg.add_row(get_row_query_transaction_items(row))

        msg_eof = msg.clone()
        msg_eof.set_as_eof(1)

        nodes_setup.select_middleware.push_msg(msg_eof)
        nodes_setup.select_middleware.push_msg(msg)

        # Topk also sent message and EOF, its divided in two types so 2 message per type == 4 messages
        self.assertEqual(len(nodes_setup.join_middleware.msgs), 4)

        # Even though received EOFS it did not send EOF to result node since not received product names
        self.assertEqual(len(nodes_setup.result_middleware.msgs), 0)

        #Message for join is hashed
        msg = CSVHashedMessageBuilder.with_credentials(["q_id"], [QUERY_PRODUCT_NAMES], "key_hash_q_id")

        # ["pr_1", "1", "100.0"], FRom topk
        # ["pr_top_q", "1", "5.0"],

        # Send for top rev, in one message just to send 2 messages
        #product_id , product_name
        msg.add_row(["pr_1", "product_name_top_rev"])

        msg_eof = msg.clone()
        msg_eof.set_as_eof(2)

        msg2 = msg.clone() # Clear payload but keep headers
        msg2.add_row(["pr_top_q", "product_name_top_qua"])


        nodes_setup.join_middleware.push_msg(msg2)

        # Still not EOF so do not send
        self.assertEqual(len(nodes_setup.result_middleware.msgs), 0)

        nodes_setup.join_middleware.push_msg(msg_eof)

        # One message still not there, should not receive anything yet.
        self.assertEqual(len(nodes_setup.result_middleware.msgs), 0)
        nodes_setup.join_middleware.push_msg(msg)

        # Finally it received everything, expected now 4 messages 2 per query type, revenue and quantity
        self.assertEqual(len(nodes_setup.result_middleware.msgs), 4)



        msg_out_rev = nodes_setup.result_middleware.msgs[0] # Since its sent on EOF then its sent on blocks 0 and 1 are for rev
        msg_out_qua = nodes_setup.result_middleware.msgs[2] 

        self.assertFalse(msg_out_rev.headers.is_eof());
        self.assertFalse(msg_out_qua.headers.is_eof());

        if msg_out_rev.headers.types != [QUERY_2_REVENUE]:
            # Just in case if not revenue then invert it
            tmp = msg_out_rev
            msg_out_rev = msg_out_qua
            msg_out_qua = tmp

        # Check TOP K for revenue
        exp_rows_q2_topk_rev = [
            #["product_name", "month", "revenue"]
            ["product_name_top_rev", "1", "100.0"], # Just top one
        ]

        res = CSVMessage(msg_out_rev.serialize_payload())
        res_rows = [row for row in res.stream_rows()]

        self.assertEqual(len(res_rows) , len(exp_rows_q2_topk_rev))
        for i in range(len(exp_rows_q2_topk_rev)):
            self.assertEqual(res_rows[i] , exp_rows_q2_topk_rev[i])


        # Check TOP K for quantity
        exp_rows_q2_topk_quantity = [
            #["product_name", "month", "quantity"]
            ["product_name_top_qua", "1", "5.0"],
        ]

        res = CSVMessage(msg_out_qua.serialize_payload())
        res_rows = [row for row in res.stream_rows()]

        self.assertEqual(len(res_rows) , len(exp_rows_q2_topk_quantity))
        for i in range(len(exp_rows_q2_topk_quantity)):
            self.assertEqual(res_rows[i] , exp_rows_q2_topk_quantity[i])


        # Check EOFS
        headers_msg_1 = nodes_setup.result_middleware.msgs[1].headers
        headers_msg_2 = nodes_setup.result_middleware.msgs[3].headers
        
        self.assertTrue(headers_msg_1.is_eof());
        self.assertEqual(headers_msg_1.msg_count, 1);
        
        self.assertTrue(headers_msg_2.is_eof());
        self.assertEqual(headers_msg_2.msg_count, 1);

        if headers_msg_1.types == [QUERY_2_REVENUE]:
            self.assertEqual(headers_msg_1.types, [QUERY_2_REVENUE])
            self.assertEqual(headers_msg_2.types, [QUERY_2_QUANTITY])
        else:
            self.assertEqual(headers_msg_2.types, [QUERY_2_REVENUE])
            self.assertEqual(headers_msg_1.types, [QUERY_2_QUANTITY])
