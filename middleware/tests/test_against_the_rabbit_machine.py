import unittest
import queue

from middleware.src.rabbitmq_middleware import RabbitQueueMiddleware, RabbitExchangeMiddleware, RabbitExchangeMiddlewareTypeNamed
from middleware.tests.helpers import wrap_callback_with_ack_handling, start_consumer_in_thread, build_message, collecting_callback, stop_consumer_and_join, safe_close,safe_async_stop_consumer_and_join

WAIT_TIMEOUT = 3.0


class MessageMiddlewareUnitTests(unittest.TestCase):

    # ---------- Comunicación por Working Queue 1 a 1 ----------
    def test_working_queue_1_to_1(self) -> None:
        # Arrange
        queue_name = "queue_1to1"
        producer = RabbitQueueMiddleware(queue_name)
        self.addCleanup(producer.close)


        received = queue.Queue()

        #consumer = RabbitQueueMiddleware(queue_name)
        create_consumer = lambda: RabbitQueueMiddleware(queue_name)
        (consumer,thread)= start_consumer_in_thread(create_consumer, wrap_callback_with_ack_handling(collecting_callback(received)))
        #self.addCleanup(consumer.stop_consuming)
        #self.addCleanup(consumer.close)

        self.addCleanup(lambda : safe_async_stop_consumer_and_join(consumer, thread, WAIT_TIMEOUT))

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
        self.addCleanup(producer.close)

        received_a, received_b = queue.Queue(), queue.Queue()

        create_consumer = lambda: RabbitQueueMiddleware(queue_name)

        (consumer_a,thread_a) = start_consumer_in_thread(create_consumer, wrap_callback_with_ack_handling(collecting_callback(received_a)))
        (consumer_b,thread_b) = start_consumer_in_thread(create_consumer, wrap_callback_with_ack_handling(collecting_callback(received_b)))

        self.addCleanup(lambda : safe_async_stop_consumer_and_join(consumer_a, thread_a, WAIT_TIMEOUT))
        self.addCleanup(lambda : safe_async_stop_consumer_and_join(consumer_b, thread_b, WAIT_TIMEOUT))
        # self.addCleanup(consumer_b.stop_consuming)
        # self.addCleanup(consumer_a.close)
        # self.addCleanup(consumer_b.close)


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
        creator_subscriber_hit  = lambda:RabbitExchangeMiddleware(rk_hit,  exchange_type)
        creator_subscriber_miss = lambda:RabbitExchangeMiddleware(rk_miss, exchange_type)



        self.addCleanup(lambda: safe_close(publisher))
        #self.addCleanup(lambda: safe_close(subscriber_miss))
        #self.addCleanup(lambda: safe_close(subscriber_hit))

        received_hit, received_miss = queue.Queue(), queue.Queue()

        (subscriber_hit,thread_hit) = start_consumer_in_thread(creator_subscriber_hit, wrap_callback_with_ack_handling(collecting_callback(received_hit)))
        (subscriber_miss,thread_miss) = start_consumer_in_thread(creator_subscriber_miss, wrap_callback_with_ack_handling(collecting_callback(received_miss)))

        self.addCleanup(lambda : safe_async_stop_consumer_and_join(subscriber_hit, thread_hit, WAIT_TIMEOUT))
        self.addCleanup(lambda : safe_async_stop_consumer_and_join(subscriber_miss, thread_miss, WAIT_TIMEOUT))

        #self.addCleanup(lambda: stop_consumer_and_join(subscriber_hit, thread_hit, WAIT_TIMEOUT))
        #self.addCleanup(lambda: stop_consumer_and_join(subscriber_miss, thread_miss, WAIT_TIMEOUT))

        # Act
        msg = build_message([["exchange_test", "direct_only_hit"]])
        publisher.send(msg)

        # Assert
        self.assertIsNotNone(received_hit.get(timeout=WAIT_TIMEOUT))
        with self.assertRaises(queue.Empty):
            received_miss.get(block=False)

    # ---------- Comunicación por Exchange 1 a N ----------
    def test_exchange_1_to_n(self) -> None:
        # Arrange
        exchange_type = "fanout"
        publisher_queue = "pub"
        sub_q1 = "s1"
        sub_q2 = "s2"

        publisher = RabbitExchangeMiddlewareTypeNamed(publisher_queue, exchange_type)
        creator_sub1 = lambda:RabbitExchangeMiddlewareTypeNamed(sub_q1, exchange_type)
        creator_sub2 = lambda:RabbitExchangeMiddlewareTypeNamed(sub_q2, exchange_type)
        self.addCleanup(publisher.close)
        
        #self.addCleanup(sub1.stop_consuming)
        #self.addCleanup(sub2.stop_consuming)
        #self.addCleanup(sub1.close)
        #self.addCleanup(sub2.close)

        received_1, received_2 = queue.Queue(), queue.Queue()

        (sub1, thread_sub1) = start_consumer_in_thread(creator_sub1, wrap_callback_with_ack_handling(collecting_callback(received_1)))
        (sub2, thread_sub2) = start_consumer_in_thread(creator_sub2, wrap_callback_with_ack_handling(collecting_callback(received_2)))

        self.addCleanup(lambda : safe_async_stop_consumer_and_join(sub1, thread_sub1, WAIT_TIMEOUT))
        self.addCleanup(lambda : safe_async_stop_consumer_and_join(sub2, thread_sub2, WAIT_TIMEOUT))

        message = build_message([["broadcast", "to_all"]])

        # Act
        publisher.send(message)

        # Assert
        self.assertIsNotNone(received_1.get(timeout=WAIT_TIMEOUT))
        self.assertIsNotNone(received_2.get(timeout=WAIT_TIMEOUT))


if __name__ == '__main__':
    unittest.main()
