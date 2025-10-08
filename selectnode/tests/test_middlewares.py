import unittest

from middleware.groupby_middleware import (
    GROUPBY_EXCHANGE,
    GROUPBY_TASKS_QUEUE_BASE,
    GroupbyTasksMiddleware,
)
from middleware.routing.csv_message import *
from middleware.routing.header_fields import *
from middleware.select_tasks_middleware import SelectTasksMiddleware
from middleware.memory_middleware import *

from common.config.row_filtering import *
from middleware import routing


def map_dict_to_vect(row):
    return [row["year"], row["hour"], row["sum"]]


def map_vect_to_dict(row):
    return {"year": int(row[0]), "hour": int(row[1]), "sum": int(row[2])}

def wait_middleware_init_nothing():
    pass

class MethodClass:
    def __init__(self, tag):
        self.delivery_tag = tag
class MessageHolder:
    def __init__(self, headers, payload):
        self.headers = headers
        self.payload = payload


class QueueConsumeObj:
    def __init__(self):
        self.msgs = []
        self.listeners = {}
        self.binds = {}

    def send(self, msg):
        for listener in self.listeners.values():
            listener(msg)

    def push(self, msg):
        self.msgs.append(msg)


class MockChannel:
    def __init__(self, host):
        self.consuming = False
        self.queues = {}
        self.host = host
        self.acked_tags = set()
        self.nacked_tags = set()

    def basic_qos(self, prefetch_count):
        pass

    def exchange_declare(self, exchange, exchange_type, durable=False):
        pass

    def declared_queues(self):
        return self.queues.keys()

    def queue_bind(self, queue, exchange, routing_key):
        if queue in self.queues:
            self.queues[queue].binds[exchange] = routing_key

    def queue_declare(self, queue):
        if queue in self.queues:
            return

        self.queues[queue] = QueueConsumeObj()

    def queue_delete(self, queue):
        if queue in self.queues:
            self.queues.pop(queue)

    def start_consuming(self):
        self.consuming = True

    def stop_consuming(self):
        self.consuming = False

    def basic_consume(self, queue, on_message_callback, auto_ack):
        self.queues[queue].listeners[self.host] = on_message_callback

    def basic_publish(self, exchange, routing_key, body, properties):
        # for now routing key == queue name
        self.queues[routing_key].push(
            {"exchange": exchange, "body": body, "props": properties}
        )

    def basic_ack(self, delivery_tag):
        self.acked_tags.add(delivery_tag)
    def basic_nack(self, delivery_tag, requeue = False):
        self.nacked_tags.add((delivery_tag, requeue))


class MockConnection:
    def __init__(self, host):
        self.host = host
        self.channels = []

    def channel(self):
        chann = MockChannel(self.host)

        self.channels.append(chann)

        return chann


class PropHeaders:
    def __init__(self, headers):
        self.headers = headers


class TestMiddlewares(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.active_conns = {}

    def mock_open_connection(self, host, attempts):
        res = MockConnection(host)
        self.active_conns[host] = res

        return res

    def setUp(self):
        routing.try_open_connection = self.mock_open_connection
        routing.build_headers = PropHeaders
        routing.wait_middleware_init = wait_middleware_init_nothing
    def test_simple_queue_declare(self):
        # routing.try_open_connection("SOME", 23)
        HOST = "test_register"
        QUEUE_NAME = "test_queue"
        middleware = SelectTasksMiddleware(host=HOST, queue_name=QUEUE_NAME)

        conn = self.active_conns.get(HOST, None)
        self.assertTrue(conn != None)

        self.assertEqual(len(conn.channels), 1)
        channel = conn.channels[0]
        declared_queues = list(channel.declared_queues())
        # Not defined yet, declared on consume start
        self.assertEqual(len(declared_queues), 1)
        self.assertFalse(channel.consuming)

    def test_simple_basic_consume(self):
        # routing.try_open_connection("SOME", 23)
        HOST = "test_basic_consume"
        QUEUE_NAME = "test_queue"
        middleware = SelectTasksMiddleware(host=HOST, queue_name=QUEUE_NAME)

        conn = self.active_conns.get(HOST, None)
        self.assertTrue(conn != None)

        self.assertTrue(len(conn.channels) == 1)
        channel = conn.channels[0]
        out_msgs = []
        middleware.start_consuming(lambda headers, payload: out_msgs.append(MessageHolder(headers, payload))) # Make it return true.

        declared_queues = list(channel.declared_queues())

        self.assertEqual(len(declared_queues), 1)
        self.assertTrue(declared_queues[0] == QUEUE_NAME)

        self.assertTrue(channel.consuming)
        self.assertEqual(len(channel.queues[QUEUE_NAME].listeners), 1)

        push_method = channel.queues[QUEUE_NAME].listeners.get(HOST, None)
        self.assertTrue(push_method != None)

        # Should push ch, method, properties, body
        exp_headers = BaseHeaders(ids = ["q1"], types= "t_1")
        exp_payload = b"123,23,345"

        push_method(
            channel,
            MethodClass("test_msg_1"),
            routing.build_headers(exp_headers.to_dict()),
            exp_payload,
        )

        self.assertEqual(len(out_msgs), 1)
        msg = out_msgs[0]
        self.assertEqual(msg.headers.to_dict(), exp_headers.to_dict())
        self.assertEqual(msg.payload, exp_payload)
        self.assertIn("test_msg_1", channel.acked_tags)
        self.assertNotIn(("test_msg_1", True), channel.nacked_tags)
        self.assertNotIn(("test_msg_1", False), channel.nacked_tags)

    def test_simple_basic_publish(self):
        # routing.try_open_connection("SOME", 23)
        HOST = "test_basic_publish"
        QUEUE_NAME = "test_queue"
        middleware = SelectTasksMiddleware(host=HOST, queue_name=QUEUE_NAME)

        conn = self.active_conns.get(HOST, None)
        self.assertTrue(conn != None)

        self.assertTrue(len(conn.channels) == 1)
        channel = conn.channels[0]

        self.assertFalse(channel.consuming)

        # Sender should not need to care about declaring the queue
        channel.queue_declare(QUEUE_NAME)

        msg_build = CSVMessageBuilder(
            BaseHeaders(["8845cdaa-d230-4453-bbdf-0e4f783045bf,76.5"], ["query_1"])
        )

        rows_pass = [
            {"year": 2024, "hour": 7, "sum": 88},
            {"year": 2025, "hour": 23, "sum": 942},
        ]

        for itm in rows_pass:
            msg_build.add_row(map_dict_to_vect(itm))

        middleware.send(msg_build)

        queue_cons = channel.queues[QUEUE_NAME]
        self.assertEqual(len(queue_cons.msgs), 1)

        msg = queue_cons.msgs[0]
        self.assertEqual(msg["exchange"], "")
        self.assertEqual(msg["body"], b"2024,7,88\n2025,23,942")
        self.assertEqual(msg["props"].headers, msg_build.get_headers())

    def test_simple_hashed_send(self):
        # routing.try_open_connection("SOME", 23)
        HOST = "test_hashed_send"
        NODE_IND = 2
        QUEUE_NAME = GROUPBY_TASKS_QUEUE_BASE.format(IND=NODE_IND)

        COUNT_NODES = 12
        middleware = GroupbyTasksMiddleware(COUNT_NODES, ind=NODE_IND, host=HOST)

        conn = self.active_conns.get(HOST, None)
        self.assertTrue(conn != None)

        self.assertTrue(len(conn.channels) == 1)
        channel = conn.channels[0]

        self.assertFalse(channel.consuming)

        msg_build = CSVHashedMessageBuilder(
            BaseHeaders(["8845cdaa-d230-4453-bbdf-0e4f783045bf,76.5"],
            ["query_1"]),
            "hash_base",
        )

        hashed_id = msg_build.hash_in(COUNT_NODES)
        TARGET_QUEUE = GROUPBY_TASKS_QUEUE_BASE.format(IND=hashed_id)

        # Sender should not need to care about declaring the queue
        channel.queue_declare(TARGET_QUEUE)

        rows_pass = [
            {"year": 2024, "hour": 7, "sum": 88},
            {"year": 2025, "hour": 23, "sum": 942},
        ]

        for itm in rows_pass:
            msg_build.add_row(map_dict_to_vect(itm))

        middleware.send(msg_build)

        queue_cons = channel.queues.get(TARGET_QUEUE, None)
        self.assertFalse(queue_cons == None)

        self.assertEqual(len(queue_cons.msgs), 1)

        msg = queue_cons.msgs[0]
        self.assertEqual(msg["exchange"], GROUPBY_EXCHANGE)
        self.assertEqual(msg["body"], b"2024,7,88\n2025,23,942")
        self.assertEqual(msg["props"].headers, msg_build.get_headers())

    def test_simple_hashed_consume(self):
        # routing.try_open_connection("SOME", 23)
        HOST = "test_hashed_send"
        NODE_IND = 2
        QUEUE_NAME = GROUPBY_TASKS_QUEUE_BASE.format(IND=NODE_IND)

        COUNT_NODES = 12
        middleware = GroupbyTasksMiddleware(COUNT_NODES, ind=NODE_IND, host=HOST)

        conn = self.active_conns.get(HOST, None)
        self.assertTrue(conn != None)

        self.assertTrue(len(conn.channels) == 1)
        channel = conn.channels[0]
        out_msgs = []
        middleware.start_consuming(lambda headers, payload: out_msgs.append(MessageHolder(headers, payload)) or True) # return true... Requeue/nack

        declared_queues = list(channel.declared_queues())

        self.assertEqual(len(declared_queues), 1)
        self.assertTrue(declared_queues[0] == QUEUE_NAME)

        self.assertEqual(
            channel.queues[QUEUE_NAME].binds.get(GROUPBY_EXCHANGE, None), QUEUE_NAME
        )

        self.assertTrue(channel.consuming)
        self.assertEqual(len(channel.queues[QUEUE_NAME].listeners), 1)

        push_method = channel.queues[QUEUE_NAME].listeners.get(HOST, None)
        self.assertTrue(push_method != None)

        # Should push ch, method, properties, body

        exp_headers = BaseHeaders(ids = ["q1"], types= "t_1")
        exp_payload = b"123,23,345"

        push_method(
            channel,
            MethodClass("test_msg_1"),
            routing.build_headers(exp_headers.to_dict()),
            exp_payload,
        )

        self.assertEqual(len(out_msgs), 1)
        msg = out_msgs[0]
        self.assertEqual(msg.headers.to_dict(), exp_headers.to_dict())
        self.assertEqual(msg.payload, exp_payload)

        self.assertNotIn("test_msg_1", channel.acked_tags)
        self.assertNotIn(("test_msg_1", False), channel.nacked_tags)
        self.assertIn(("test_msg_1", True), channel.nacked_tags)


    def test_memory_middleware_delegates_builder_and_sends_msg(self):
        middleware = MemoryMiddleware()

        msgs = []

        middleware.start_consuming(lambda headers, payload: msgs.append(MemoryMessage(payload)))

        msg_build = CSVMessageBuilder(
            BaseHeaders(["8845cdaa-d230-4453-bbdf-0e4f783045bf,76.5"], ["query_1"])
        )

        rows_pass = [
            {"year": 2024, "hour": 7, "sum": 88},
            {"year": 2025, "hour": 23, "sum": 942},
            {"year": 2024, "hour": 6, "sum": 942},
            {"year": 2027, "hour": 7, "sum": 88},
            {"year": 2025, "hour": 24, "sum": 942},
            {"year": 2024, "hour": 6, "sum": 55},
        ]

        for itm in rows_pass:
            msg_build.add_row(map_dict_to_vect(itm))

        exp_payload = list(msg_build.payload)

        middleware.send(msg_build)
        
        self.assertEqual(1, len(msgs))
        res_msg= msgs[0]
        self.assertEqual(res_msg.payload, exp_payload)



    def test_memory_middleware_delegates_builder_and_sends_msg_no_serialization(self):
        middleware = MemoryMiddleware()

        msgs = []

        middleware.start_consuming(lambda h, payload: msgs.append(MemoryMessage(payload)))

        msg_build = HashedMemoryMessageBuilder.with_credentials("8845cdaa-d230-4453-bbdf-0e4f783045bf,76.5", "query_1")

        rows_pass = [
            {"year": 2024, "hour": 7, "sum": 88},
            {"year": 2025, "hour": 23, "sum": 942},
            {"year": 2024, "hour": 6, "sum": 942},
            {"year": 2027, "hour": 7, "sum": 88},
            {"year": 2025, "hour": 24, "sum": 942},
            {"year": 2024, "hour": 6, "sum": 55},
        ]

        for itm in rows_pass:
            msg_build.add_row(itm)

        middleware.send(msg_build)
        
        self.assertEqual(1, len(msgs))
        res_msg= msgs[0]
        res = [itm for itm in res_msg.stream_rows()]

        self.assertTrue(len(rows_pass) == len(res))

        for i in range(len(rows_pass)):
            self.assertTrue(rows_pass[i] == res[i])        
