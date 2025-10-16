import unittest

from middleware.groupby_middleware import (
    GROUPBY_EXCHANGE,
    GROUPBY_TASKS_QUEUE_BASE,
    GroupbyTasksMiddleware,
)
from middleware.routing.csv_message import *
from middleware.routing.header_fields import *
from middleware.select_tasks_middleware import SelectTasksMiddleware, SelectTasksMultiMiddleware, SELECT_TASKS_EXCHANGE
from middleware.memory_middleware import *

from common.config.row_filtering import *
from middleware.rabbitmq import utils as rbmq_utils


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

    def basic_publish(self, exchange, routing_key, body, properties, mandatory = False):
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


class TestMiddlewaresMulti(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.active_conns = {}

    def mock_open_connection(self, host, attempts):
        res = MockConnection(host)
        self.active_conns[host] = res

        return res

    def setUp(self):
        rbmq_utils.try_open_connection = self.mock_open_connection
        rbmq_utils.build_headers = PropHeaders
        rbmq_utils.wait_middleware_init = wait_middleware_init_nothing
    def test_simple_queue_declare_multi(self):
        # rbmq_utils.try_open_connection("SOME", 23)
        HOST = "test_register"
        QUEUE_NAME = "test_queue"
        middleware = SelectTasksMultiMiddleware(host=HOST, queue_names=[QUEUE_NAME])

        conn = self.active_conns.get(HOST, None)
        self.assertTrue(conn != None)

        self.assertEqual(len(conn.channels), 1)
        channel = conn.channels[0]
        declared_queues = list(channel.declared_queues())
        # Not defined yet, declared on consume start
        self.assertEqual(len(declared_queues), 1)
        self.assertFalse(channel.consuming)

    def test_simple_basic_consume_multi(self):
        HOST = "test_basic_consume"
        QUEUE_NAME = "test_queue"
        middleware = SelectTasksMultiMiddleware(host=HOST, queue_names=[QUEUE_NAME])

        conn = self.active_conns.get(HOST, None)
        self.assertTrue(conn != None)

        self.assertTrue(len(conn.channels) == 1)
        channel = conn.channels[0]
        out_msgs = []
        middleware.start_consuming({
            QUEUE_NAME: lambda headers, payload: out_msgs.append(MessageHolder(headers, payload))
        })

        declared_queues = list(channel.declared_queues())

        self.assertEqual(len(declared_queues), 1)
        

        REAL_QUEUE_NAME = SelectTasksMultiMiddleware.parse_queue_name_select(QUEUE_NAME)

        self.assertEqual(declared_queues[0], REAL_QUEUE_NAME)
        self.assertTrue(channel.consuming)

        self.assertEqual(len(channel.queues[REAL_QUEUE_NAME].listeners), 1)

        push_method = channel.queues[REAL_QUEUE_NAME].listeners.get(HOST, None)
        self.assertTrue(push_method != None)

        # Should push ch, method, properties, body
        exp_headers = BaseHeaders(ids = ["q1"])
        exp_payload = b"123,23,345"

        push_method(
            channel,
            MethodClass("test_msg_1"),
            rbmq_utils.build_headers(exp_headers.to_dict()),
            exp_payload,
        )

        exp_headers.types = [QUEUE_NAME]

        self.assertEqual(len(out_msgs), 1)
        msg = out_msgs[0]
        self.assertEqual(msg.headers.to_dict(), exp_headers.to_dict())
        self.assertEqual(msg.payload, exp_payload)
        self.assertIn("test_msg_1", channel.acked_tags)
        self.assertNotIn(("test_msg_1", True), channel.nacked_tags)
        self.assertNotIn(("test_msg_1", False), channel.nacked_tags)

    def test_simple_basic_publish_multi(self):
        HOST = "test_basic_publish"
        QUEUE_NAME = "test_queue"
        middleware = SelectTasksMultiMiddleware(host=HOST, queue_names=[QUEUE_NAME])

        conn = self.active_conns.get(HOST, None)
        self.assertTrue(conn != None)

        self.assertTrue(len(conn.channels) == 1)
        channel = conn.channels[0]

        self.assertFalse(channel.consuming)

        # Sender should not need to care about declaring the queue
        channel.queue_declare(QUEUE_NAME)

        msg_build = CSVMessageBuilder(
            BaseHeaders(["8845cdaa-d230-4453-bbdf-0e4f783045bf,76.5"], [QUEUE_NAME])
        )

        rows_pass = [
            {"year": 2024, "hour": 7, "sum": 88},
            {"year": 2025, "hour": 23, "sum": 942},
        ]

        for itm in rows_pass:
            msg_build.add_row(map_dict_to_vect(itm))

        middleware.send(msg_build)
        REAL_QUEUE_NAME = SelectTasksMultiMiddleware.parse_queue_name_select(QUEUE_NAME)

        queue_cons = channel.queues[REAL_QUEUE_NAME]
        self.assertEqual(len(queue_cons.msgs), 1)

        msg = queue_cons.msgs[0]
        self.assertEqual(msg["exchange"], SELECT_TASKS_EXCHANGE)
        self.assertEqual(msg["body"], b"2024,7,88\n2025,23,942")
        self.assertEqual(msg["props"].headers, msg_build.headers.to_dict_no_type())