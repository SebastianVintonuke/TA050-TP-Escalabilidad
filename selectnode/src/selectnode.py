# from .type_config import TypeConfiguration
import logging


class TypeHandler:
    def __init__(self, type_conf, msg_builder):
        self.type_conf = type_conf
        self.msg_builder = msg_builder
    
    def check(self, row):
        self.type_conf.filter_map(row, self.msg_builder)
    
    def send_built(self):
        self.type_conf.send(self.msg_builder)


class SelectNode:
    def __init__(self, select_middleware, payload_deserializer, type_expander):
        self.middleware = select_middleware
        self.type_expander = type_expander
        self.payload_deserializer = payload_deserializer

    def handle_task(self, headers, msg):
        msg = self.payload_deserializer(msg)

        if msg.is_empty():  # Empty msg is signal of EOF or error, depending on headers.
            self.type_expander.propagate_signal_in(headers)
            return True

        outputs = []
        types = set()
        for type_header in headers.split():
            for config in self.type_expander.get_configurations_for(type_header.types[0]):
                outputs.append(TypeHandler(config,
                    config.new_builder_for(type_header)))

        for row in msg.stream_rows():
            for output in outputs:
                output.check(row)

        for output in outputs:
            output.send_built()

        return True

    def start(self):
        self.middleware.start_consuming(self.handle_task)

    def close(self):
        self.middleware.close()
        self.type_expander.close()
