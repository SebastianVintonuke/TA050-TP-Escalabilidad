# from .type_config import TypeConfiguration
import logging
from .join_accumulator import JoinAccumulator
class JoinNode:
    def __init__(self, join_middleware, type_expander):
        self.middleware = join_middleware
        self.type_expander = type_expander
        self.joiners = {}

    def handle_task(self, msg):
        if msg.is_partition_eof(): # Partition EOF is sent when no more data on partition, or when real EOF or error happened as signal.
            if msg.is_last_message():
                if msg.is_eof():
                    logging.info(f"Received final eof OF {msg.ids} types: {msg.types}")
                    ind=0
                    type = msg.types[ind]
                    ide = "" #msg.types[ind]
                    
                    for config in self.type_expander.get_configurations_for(type):
                        joiner = self.joiners.get(ide+config.join_id, None)
                        if joiner:
                            if config.left_type == type:
                                if joiner.handle_eof_left(): #Finished
                                    self.joiners.pop(ide+config.join_id)
                                    
                            elif joiner.handle_eof_right(): #Finished
                                    self.joiners.pop(ide+config.join_id)
                        else:
                            # propagate eof signal for this message 
                            config.send(config.new_builder_for(msg, ind))
                else:
                    logging.info(f"Received ERROR code: {msg.partition} IN {msg.ids}")
                    self.type_expander.propagate_signal_in(msg)
            else: # Not last message, mark partition as ended
                pass
            msg.ack_self()
            return

        row_actions = []
        ind = 0
        type = msg.types[ind]
        ide = ""#msg.ids[ind]

        for config in self.type_expander.get_configurations_for(type):
            joiner = self.joiners.get(ide+config.join_id, None)

            if joiner == None:
                joiner = JoinAccumulator(config, msg, ind)
                self.joiners[ide+config.join_id] = joiner

            row_actions.append(joiner.get_action_for_type(type))

        for row in msg.stream_rows():
            for action in row_actions:
                action(row)

        msg.ack_self()

    def start(self):
        self.middleware.start_consuming(self.handle_task)

    def close(self):
        self.middleware.close()
        self.type_expander.close()
