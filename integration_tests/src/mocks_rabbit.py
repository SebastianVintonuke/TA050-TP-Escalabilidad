
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

    def send_kwargs(self, **params):
        for listener in self.listeners.values():
            listener(**params)

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
        print(f"ATTEMPT CONSUME AT queue {queue} {on_message_callback}")
        self.queues[queue].listeners[self.host] = on_message_callback

    def basic_publish(self, exchange, routing_key, body, properties):
        print(f"PUBLISH AT key {routing_key} exchange:{exchange} {properties.headers}")

        queue_obj=self.queues[routing_key]
        #ch, method, properties, body
        queue_obj.send_kwargs(
            ch = self,
            method= MethodClass(f"m_{len(queue_obj.msgs)}"),
            properties=properties,
            body=body)

        # for now routing key == queue name
        queue_obj.push(
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

def wait_middleware_init_nothing():
    pass
