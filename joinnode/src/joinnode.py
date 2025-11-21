# from .type_config import TypeConfiguration
import logging
from .join_accumulator import JoinAccumulator
from .join_state_manager import JoinNodeStateManager

from common.state_storage.nothing_state_storage import  NothingQueryStateStorage
def get_credentials(accum_id):
    parts = accum_id.split("_")

    return "_".join(parts[:-1]), parts[-1]

class JoinNode:
    def __init__(self, join_middleware, payload_deserializer, type_expander, store_creator = NothingQueryStateStorage, batch_size = 1):
        self.middleware = join_middleware
        self.type_expander = type_expander
        self.payload_deserializer = payload_deserializer
        self.joiners = {}

        # This does not load anything yet.. at start we do.
        self.state_storage = store_creator(JoinNodeStateManager(self.get_join_accumulator)) # Have it hardcoded for now
        self.batch_size = batch_size        

    def get_config_from_joiner_type_id(self, type_id):
        for config in type_expander.type_configurations:
            if config.join_id == type_id:
                return config

        raise Exception(f"'{type_id}' was not registered in any configuration")

    def get_join_accumulator(self, joiner_id):
        joiner = self.joiners.get(joiner_id, None)
        if joiner == None:

            query_id, join_type_id = get_credentials(joiner_id)



            logging.info(f"Get new Join accumulator initialization for id {joiner_id}, src type: {q_type} join type {join_type_id}")
            config = self.get_config_from_joiner_type_id(join_type_id)

            # Join config new builder for... ignores basically types field but requires a value
            new_headers = BaseHeaders(ids = [query_id], types= [join_type_id])

            joiner = JoinAccumulator(config, config.new_builder_for(new_headers), ide = joiner_id)

            self.joiners[joiner_id] = joiner       
        return joiner

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



    def handle_task(self, headers, msg):
        if headers.is_eof(): # Partition EOF is sent when no more data on partition, or when real EOF or error happened as signal.
            if headers.is_error():
                logging.info(f"Received ERROR code: {headers.get_error_code()} IN {headers.ids}")
                self.type_expander.propagate_signal_in(headers)
                return

            logging.info(f"Received final eof OF {headers.ids} types: {headers.types}")
            ind=0
            type = headers.types[ind]
            ide = headers.ids[ind]
            
            for config in self.type_expander.get_configurations_for(type):
                ide_curr = f"{ide}_{config.join_id}"
                joiner = self.joiners.get(ide_curr, None)
                if joiner == None:
                    logging.info(f"For type {type}, eof was the first message to be received")

                    joiner = JoinAccumulator(config, config.new_builder_for(headers.sub_for(ind)), ide = ide_curr)
                    self.joiners[ide_curr] = joiner
                    self.state_storage.register_query(ide_curr, joiner)
                
                if config.left_type == type:
                    if joiner.handle_eof_left(headers.msg_count): #check wether count msgs is all for left or eof reached before.
                        logging.info(f"Freeing {ide_curr}, handling done.")
                        del self.joiners[ide_curr]
                elif joiner.handle_eof_right(headers.msg_count): #Finished
                        logging.info(f"Freeing {ide_curr}, handling done.")
                        del self.joiners[ide_curr]
            
            return
        msg = self.payload_deserializer(msg) 

        row_actions = []
        checkers = []
        ind = 0
        type = headers.types[ind]
        ide = headers.ids[ind]

        for config in self.type_expander.get_configurations_for(type):
            ide_curr=f"{ide}_{config.join_id}"
            joiner = self.joiners.get(ide_curr, None)

            if joiner == None:
                joiner = JoinAccumulator(config, config.new_builder_for(headers.sub_for(ind)), ide = ide_curr)
                self.joiners[ide_curr] = joiner
                self.state_storage.register_query(ide_curr, joiner)

            #count_checker, row_action = joiner.get_action_for_type(type)
            row_actions.append(joiner.get_action_for_type(type))
            checkers.append(joiner)

        for row in msg.stream_rows():
            for action in row_actions:
                action(row)


        will_ack = False
        for joiner in checkers:
            if joiner.add_check_msg_for_type(type):
                logging.info(f"Freeing {joiner.join_id}, handling done.")
                del self.joiners[joiner.join_id]
                will_ack = True
            else:
                joiner.batch_msg_count+=1 
                will_ack = will_ack or joiner.batch_msg_count >= self.batch_size


        if will_ack:
            # Only save/push on batch size count... If any on the message did have count on batch size...
            # Actually only time msg is shared in multi types is for the menu item names and store names... Not often.

            for joiner in checkers:
                mock_pkt_id = joiner.msg_count_left + joiner.msg_count_right
                self.state_storage.write_changes(joiner.join_id, mock_pkt_id, joiner) # Save state
                self.state_storage.commit_changes(joiner.join_id)
                self.state_storage.push_changes(joiner.join_id, mock_pkt_id, joiner, joiner.batch_msg_count)
                joiner.batch_msg_count = 0

            return # Ack batch messages

        return True # Return true to accumulate in batch.



    def start(self):
        # Even If there is no pending changes, load states of queries, to continue on memory and not load always from disk.
        self.state_storage.load_states() 
        self.state_storage.check_integrity()

        self.middleware.start_consuming(self.handle_task)

    def start_on(self, middleware):
        middleware.start_consuming(self.handle_task)

    def close(self):
        self.middleware.close()
        self.type_expander.close()
