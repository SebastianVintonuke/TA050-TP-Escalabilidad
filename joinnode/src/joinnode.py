# from .type_config import TypeConfiguration
import logging
from .join_accumulator import JoinAccumulator
class JoinNode:
    def __init__(self, join_middleware, type_expander):
        self.middleware = join_middleware
        self.type_expander = type_expander
        self.joiners = {}


    def len_in_progress(self):
        return len(self.joiners)

    def len_input_rows(self):
        total =0 
        for _, joiner in self.joiners.items():
            total+= joiner.len_left() + joiner.len_right()
        return total

    def len_out_rows(self):
        total =0 
        for _, joiner in self.joiners.items():
            total+= joiner.len_joined()
        return total

    def len_left_rows(self):
        total =0 
        for _, joiner in self.joiners.items():
            total+= joiner.len_left()
        return total



    def handle_task(self, msg):
        if msg.is_eof(): # Partition EOF is sent when no more data on partition, or when real EOF or error happened as signal.
            if msg.is_error():
                logging.info(f"Received ERROR code: {msg.partition} IN {msg.ids}")
                self.type_expander.propagate_signal_in(msg)
                return

            logging.info(f"Received final eof OF {msg.ids} types: {msg.types}")
            ind=0
            type = msg.types[ind]
            ide = msg.ids[ind]
            
            for config in self.type_expander.get_configurations_for(type):
                joiner = self.joiners.get(ide+config.join_id, None)
                if joiner == None:
                    logging.info(f"For type {type}, eof was the first message to be received")

                    joiner = JoinAccumulator(config, msg, ind)
                    self.joiners[ide+config.join_id] = joiner
                
                if config.left_type == type:
                    if joiner.handle_eof_left(msg.partition): #Finished? msg.partition == count of messages that should have been here
                        logging.info(f"Freeing {ide+config.join_id}, handling done.")
                        del self.joiners[ide+config.join_id]
                elif joiner.handle_eof_right(msg.partition): #Finished
                        logging.info(f"Freeing {ide+config.join_id}, handling done.")
                        del self.joiners[ide+config.join_id]
            
            return

        row_actions = []
        checkers = []
        ind = 0
        type = msg.types[ind]
        ide = msg.ids[ind]

        for config in self.type_expander.get_configurations_for(type):
            joiner = self.joiners.get(ide+config.join_id, None)

            if joiner == None:
                joiner = JoinAccumulator(config, msg, ind)
                self.joiners[ide+config.join_id] = joiner

            #count_checker, row_action = joiner.get_action_for_type(type)
            row_actions.append(joiner.get_action_for_type(type))
            checkers.append(joiner)

        for row in msg.stream_rows():
            for action in row_actions:
                action(row)


        for joiner in checkers:
            if joiner.add_check_msg_for_type(type):
                logging.info(f"Freeing {ide+config.join_id}, handling done.")
                del self.joiners[ide+joiner.type_conf.join_id]


    def start(self):
        self.middleware.start_consuming(self.handle_task)

    def start_on(self, middleware):
        middleware.start_consuming(self.handle_task)

    def close(self):
        self.middleware.close()
        self.type_expander.close()
