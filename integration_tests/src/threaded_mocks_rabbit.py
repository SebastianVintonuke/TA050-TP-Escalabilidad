from .threaded_handling import AsyncConsumerListener
import time
import threading

class MethodClass:
    def __init__(self, tag):
        self.delivery_tag = tag
class MessageHolder:
    def __init__(self, headers, payload):
        self.headers = headers
        self.payload = payload


class QueueConsumeObj:
    def __init__(self, time_per_send = 0):
        self.msgs = []
        self.listeners = {}
        self.binds = {}
        self.time_per_send = 0

    def send(self, msg):
        for listener in self.listeners.values():
            listener(msg)

    def send_kwargs(self, **params):
        for listener in self.listeners.values():
            listener(**params)

    def push(self, msg):
        self.msgs.append(msg)

    def add_listener(self, ide, callback):
        self.listeners[ide] = callback
    def remove_listener(self, ide):
        self.listeners.pop(ide, None)

    def force_remove_listener(self, ide):
        self.remove_listener(ide)
    def close(self):
        pass
    def force_close(self):
        pass


class AsyncQueue(QueueConsumeObj):
    def __init__(self, time_per_send = 0):
        super().__init__(time_per_send)

    def sleep_and_handle(self, callback):
        def sleep_and_do(obj):
            time.sleep(self.time_per_send)
            callback(obj)
        return sleep_and_do


    def send_kwargs(self, **params):
        for listener in self.listeners.values():
            listener.put(params)


    def add_listener(self, ide, callback, max_size = 128):
        if self.time_per_send > 0:
            callback = self.sleep_and_handle(callback)

        if ide in self.listeners:
            self.listeners[ide].listener = callback
        else:
            listener = AsyncConsumerListener(callback, max_size = max_size)
            listener.start()
            self.listeners[ide] = listener

    def remove_listener(self, ide):
        listener = self.listeners.pop(ide, None)
        if listener:
            listener.stop()

    def close(self):
        for listener in self.listeners.values():
            listener.stop()

    def force_remove_listener(self, ide):
        listener = self.listeners.pop(ide, None)
        if listener:
            listener.force_stop()

    def force_close(self):
        for listener in self.listeners.values():
            listener.force_stop()



class HostManager:
    def __init__(self, name):
        self.name= name
        self.queues = {}
        self.lock_queues_define = threading.Lock()
        self.channel_count = 0
        self.queue_type = AsyncQueue
        self.time_per_send = 0

    def get_channel_id(self):
        with self.lock_queues_define:
            res = self.channel_count
            self.channel_count+=1
            return res


    def declared_queues(self):
        with self.lock_queues_define:
            return self.queues.keys()

    def queue_bind(self, queue, exchange, routing_key):
        with self.lock_queues_define:
            if queue in self.queues:
                self.queues[queue].binds[exchange] = routing_key

    def queue_declare(self, queue):
        with self.lock_queues_define:
            if queue in self.queues:
                return

            self.queues[queue] = self.queue_type()

    def queue_delete(self, queue):
        with self.lock_queues_define:
            if queue in self.queues:
                queue_obj = self.queues.pop(queue, None)
                if queue_obj:
                    queue_obj.close()


    def exchange_declare(self, exchange, exchange_type, durable=False):
        pass


    def basic_consume(self, channel_id, queue, on_message_callback, auto_ack):
        print(f"{channel_id} CONSUME AT queue {queue} {on_message_callback}")
        with self.lock_queues_define:
            self.queues[queue].add_listener(channel_id, on_message_callback)

    def stop_consuming(self, channel_id):
        print(f"STOP CONSUMING {channel_id}")
        with self.lock_queues_define:
            for queue in self.queues.values():
                queue.remove_listener(channel_id) # Drop listener for channel id If there is one.

    def basic_publish(self, chann, exchange, routing_key, body, properties):
        #print(f"PUBLISH AT key {routing_key} exchange:{exchange} {properties.headers} sleep {self.time_per_send}")
        time.sleep(self.time_per_send)

        with self.lock_queues_define:
            queue_obj=self.queues[routing_key]
            #ch, method, properties, body
            queue_obj.send_kwargs(
                ch = chann,
                method= MethodClass(f"m_{len(queue_obj.msgs)}"),
                properties=properties,
                body=body)

            # for now routing key == queue name
            queue_obj.push(
                {"exchange": exchange, "body": body, "props": properties}
            )

class BasicChannel:
    def __init__(self, host):
        self.consuming = False
        self.host = host

        self.id = self.host.get_channel_id()

        # Delegate but faster?
        self.declared_queues = self.host.declared_queues
        self.queue_bind = self.host.queue_bind
        self.queue_declare = self.host.queue_declare
        self.queue_declare = self.host.queue_declare
        self.queue_delete = self.host.queue_delete
        self.queue_delete = self.host.queue_delete
        self.exchange_declare = self.host.exchange_declare
        self.is_open = True

        self.declared_consumes = []

        # Not important for now!
        #self.acked_tags = set()
        #self.nacked_tags = set()


    def close(self):
        if self.consuming:
            self.stop_consuming()
        
        self.is_open = False 
        self.declared_consumes.clear()


    def basic_qos(self, prefetch_count):
        pass

    def start_consuming(self):
        self.consuming = True
        for (queue, on_message_callback, auto_ack) in self.declared_consumes:
            self.host.basic_consume(self.id, queue, on_message_callback, auto_ack)

    def stop_consuming(self):
        self.consuming = False
        self.host.stop_consuming(self.id)

    # Do not add it yet.
    def basic_consume(self, queue, on_message_callback, auto_ack):
        self.declared_consumes.append((queue, on_message_callback, auto_ack))
    
    def basic_publish(self, exchange, routing_key, body, properties):
        self.host.basic_publish(self, exchange, routing_key, body, properties)

    def basic_ack(self, delivery_tag):
        pass#self.acked_tags.add(delivery_tag)
    def basic_nack(self, delivery_tag, requeue = False):
        pass#self.nacked_tags.add((delivery_tag, requeue))


class MockConnection:
    def __init__(self, host_manager):
        self.host = host_manager.name
        self.host_manager = host_manager
        self.channels = []
        self.is_open = True

    def channel(self):
        chann = BasicChannel(self.host_manager)

        self.channels.append(chann)

        return chann

    def close(self):
        self.is_open = False


class PropHeaders:
    def __init__(self, headers):
        self.headers = headers

def wait_middleware_init_nothing():
    pass
