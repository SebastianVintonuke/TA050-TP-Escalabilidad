# from .type_config import TypeConfiguration
import logging
from middleware.routing.query_types import *

class TypeHandler:
    def __init__(self, type_conf, msg_builder):
        self.type_conf = type_conf
        self.msg_builder = msg_builder
    
    def check(self, row):
        self.type_conf.filter_map(row, self.msg_builder)
    
    def send_built(self):
        #logging.info(f"----------> {self.msg_builder.headers.types} message sent len:{self.msg_builder.len_payload()}")
        self.type_conf.send(self.msg_builder)


class SelectNode:
    def __init__(self, select_middleware, payload_deserializer, type_expander):
        self.middleware = select_middleware
        self.type_expander = type_expander
        self.payload_deserializer = payload_deserializer

    def handle_task(self, headers, msg):
        if headers.is_eof():  # Empty msg is signal of EOF or error, depending on headers.
            logging.info(f"Select node propagating eof of {headers}")
            self.type_expander.propagate_signal_in(headers)
            return False
            
        msg = self.payload_deserializer(msg)
        outputs = []
        for type_header in headers.split():
            for config in self.type_expander.get_configurations_for(type_header.types[0]):
                outputs.append(TypeHandler(config,
                    config.new_builder_for(type_header.clone())))

        for row in msg.stream_rows():
            for output in outputs:
                output.check(row)

        for output in outputs:
            output.send_built()

        return False

    def start(self):
        self.start_single()
                
    def start_multi(self):
        self.middleware.start_consuming({
            QUERY_1: self.handle_task,
            QUERY_2: self.handle_task,
            QUERY_3: self.handle_task,
            QUERY_4: self.handle_task,
            ALL_FOR_TRANSACTIONS: self.handle_task,
            ALL_FOR_TRANSACTIONS_ITEMS:self.handle_task
        })
        #self.middleware.start_consuming(self.handle_task)

    def start_single(self):
        self.middleware.start_consuming(self.handle_task)

    def close(self):
        self.middleware.close()
        self.type_expander.close()
