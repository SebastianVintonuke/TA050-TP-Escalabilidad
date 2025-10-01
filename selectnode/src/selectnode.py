# from .type_config import TypeConfiguration
import logging


class TypeHandler:
    def __init__(self, type_conf, msg_builder):
        self.type_conf = type_conf
        self.msg_builder = msg_builder

    def __init__(self, type_conf, msg, ind):
        self.type_conf = type_conf
        self.msg_builder = type_conf.new_builder_for(msg, ind)

    def check(self, row):
        row = self.type_conf.filter_map(row)
        if row != None:
            self.msg_builder.add_row(row)

    def send_built(self):
        if self.msg_builder.has_payload():
            self.type_conf.send(self.msg_builder)
        else:
            pass
            # logging.info(f"action: filtered_full_msg | result: success | complete message for {self.msg_builder.types} was filtered")


class SelectNode:
    def __init__(self, select_middleware, type_expander):
        self.middleware = select_middleware
        self.type_expander = type_expander

    def handle_task(self, msg):
        # msg.describe()

        if (
            msg.is_partition_eof()
        ):  # Partition EOF is sent when no more data on partition, or when real EOF or error happened as signal.
            self.type_expander.propagate_signal_in(msg)
            return
            # logging.info(f"Should handle EOF, or error in send, code {msg.partition}")

        outputs = []
        ind = 0
        types = set()
        for type in msg.types:
            for config in self.type_expander.get_configurations_for(type):
                outputs.append(TypeHandler(config, msg, ind))
            ind += 1

        for row in msg.stream_rows():
            for output in outputs:
                output.check(row)

        for output in outputs:
            output.send_built()

    def start(self):
        self.middleware.start_consuming(self.handle_task)

    def close(self):
        self.middleware.close()
        self.type_expander.close()
