import unittest
import queue

from typing import Any

from middleware.src.rabbitmq_middleware import RabbitQueueMiddleware, RabbitExchangeMiddleware
from middleware.tests.helpers import wrap_callback_with_ack_handling, start_consumer_in_thread, build_message, collecting_callback

WAIT_TIMEOUT = 3.0


class MessageMiddlewareUnitTests(unittest.TestCase):

    # ---------- Comunicación por Working Queue 1 a 1 ----------
    def test_working_queue_1_to_1(self) -> None:
        # Arrange
        queue_name = "queue_1to1"
        producer = RabbitQueueMiddleware(queue_name)
        consumer = RabbitQueueMiddleware(queue_name)
        self.addCleanup(consumer.stop_consuming)
        self.addCleanup(producer.close)
        self.addCleanup(consumer.close)

        received = queue.Queue()

        start_consumer_in_thread(consumer, wrap_callback_with_ack_handling(collecting_callback(received)))
        message = build_message([["k", "v"]])

        # Act
        producer.send(message)

        # Assert
        self.assertIsNotNone(received.get(timeout=WAIT_TIMEOUT))

    # ---------- Comunicación por Working Queue 1 a N ----------
    def test_working_queue_1_to_n(self) -> None:
        # Arrange
        queue_name = "queue_1toN"
        producer = RabbitQueueMiddleware(queue_name)
        consumer_a = RabbitQueueMiddleware(queue_name)
        consumer_b = RabbitQueueMiddleware(queue_name)
        self.addCleanup(consumer_a.stop_consuming)
        self.addCleanup(consumer_b.stop_consuming)
        self.addCleanup(producer.close)
        self.addCleanup(consumer_a.close)
        self.addCleanup(consumer_b.close)

        received_a, received_b = queue.Queue(), queue.Queue()

        start_consumer_in_thread(consumer_a, wrap_callback_with_ack_handling(collecting_callback(received_a)))
        start_consumer_in_thread(consumer_b, wrap_callback_with_ack_handling(collecting_callback(received_b)))

        # Act
        producer.send(build_message([["msg_id", "0"]]))
        producer.send(build_message([["msg_id", "1"]]))

        # Assert
        self.assertIsNotNone(received_a.get(timeout=WAIT_TIMEOUT))
        self.assertIsNotNone(received_b.get(timeout=WAIT_TIMEOUT))

    # ---------- Comunicación por Exchange 1 a 1 ----------
    def test_exchange_1_to_1(self) -> None:
        # Arrange
        exchange_type = "direct"
        rk_hit = "rk_hit"
        rk_miss = "rk_miss"

        publisher = RabbitExchangeMiddleware(rk_hit, exchange_type)
        sub_hit = RabbitExchangeMiddleware(rk_hit, exchange_type)
        sub_miss = RabbitExchangeMiddleware(rk_miss, exchange_type)
        self.addCleanup(sub_hit.stop_consuming)
        self.addCleanup(sub_miss.stop_consuming)
        self.addCleanup(publisher.close)
        self.addCleanup(sub_hit.close)
        self.addCleanup(sub_miss.close)

        received_hit, received_miss = queue.Queue(), queue.Queue()

        start_consumer_in_thread(sub_hit,  wrap_callback_with_ack_handling(collecting_callback(received_hit)))
        start_consumer_in_thread(sub_miss, wrap_callback_with_ack_handling(collecting_callback(received_miss)))

        message = build_message([["exchange_test", "direct_only_hit"]])

        # Act
        publisher.send(message)

        # Assert
        self.assertIsNotNone(received_hit.get(timeout=WAIT_TIMEOUT))

    # ---------- Comunicación por Exchange 1 a N ----------
    def test_exchange_1_to_n(self) -> None:
        # Arrange
        exchange_type = "fanout"
        publisher_queue = "pub"
        sub_q1 = "s1"
        sub_q2 = "s2"

        publisher = RabbitExchangeMiddleware(publisher_queue, exchange_type)
        sub1 = RabbitExchangeMiddleware(sub_q1, exchange_type)
        sub2 = RabbitExchangeMiddleware(sub_q2, exchange_type)
        self.addCleanup(sub1.stop_consuming)
        self.addCleanup(sub2.stop_consuming)
        self.addCleanup(publisher.close)
        self.addCleanup(sub1.close)
        self.addCleanup(sub2.close)

        received_1, received_2 = queue.Queue(), queue.Queue()

        start_consumer_in_thread(sub1, wrap_callback_with_ack_handling(collecting_callback(received_1)))
        start_consumer_in_thread(sub2, wrap_callback_with_ack_handling(collecting_callback(received_2)))

        message = build_message([["broadcast", "to_all"]])

        # Act
        publisher.send(message)

        # Assert
        self.assertIsNotNone(received_1.get(timeout=WAIT_TIMEOUT))
        self.assertIsNotNone(received_2.get(timeout=WAIT_TIMEOUT))


if __name__ == '__main__':
    unittest.main()
