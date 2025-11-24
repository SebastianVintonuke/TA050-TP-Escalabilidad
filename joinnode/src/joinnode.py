# from .type_config import TypeConfiguration
import logging

from middleware.routing.header_fields import BaseHeaders
from .join_accumulator import JoinAccumulator
from .join_state_manager import JoinNodeStateManager

from common.state_storage.nothing_state_storage import  NothingQueryStateStorage

from middleware.routing import batch_ack_actions as batch_actions

log_info = logging.info
# log_info = print
def get_credentials(accum_id):
    parts = accum_id.split("_")

    return "_".join(parts[:-1]), parts[-1]

class JoinNode:
    def __init__(self, join_middleware, payload_deserializer, type_expander, store_creator = NothingQueryStateStorage, batch_size = 1, limit = 1000):
        self.middleware = join_middleware
        self.type_expander = type_expander
        self.payload_deserializer = payload_deserializer
        self.joiners = {}

        # This does not load anything yet.. at start we do.
        self.state_storage = store_creator(JoinNodeStateManager(self.get_join_accumulator)) # Have it hardcoded for now
        self.batch_size = batch_size        
        self.msg_rows_limit= limit

    def describe(self):
        for ide, acc in self.joiners.items():
            log_info(f"--------> {ide}")
            acc.describe()

    def get_acc_id(self, query_id, joiner_id):
        return f"{query_id}_{joiner_id}"

    def get_partial_result_from(self, curr_state):
        return curr_state.get_partial_result()

    def get_config_from_joiner_type_id(self, type_id):
        for config in self.type_expander.type_configurations:
            if config.join_id == type_id:
                return config

        raise Exception(f"'{type_id}' was not registered in any configuration")

    def get_join_accumulator(self, joiner_id):
        joiner = self.joiners.get(joiner_id, None)
        if joiner == None:

            query_id, join_type_id = get_credentials(joiner_id)

            log_info(f"Get new Join accumulator initialization for id {joiner_id},  join type {join_type_id}")
            config = self.get_config_from_joiner_type_id(join_type_id)

            # Join config new builder for... ignores basically types field but requires a value
            new_headers = BaseHeaders(ids = [query_id], types= [join_type_id])

            joiner = JoinAccumulator(config, config.new_builder_for(new_headers), ide = joiner_id, limit = self.msg_rows_limit)

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

        query_headers = headers.first_query() # Should only be one for groupby/topk
        type = query_headers.types[0]
        ide = query_headers.ids[0]
        configs = self.type_expander.get_configurations_for(type)

        if query_headers.is_eof(): # Partition EOF is sent when no more data on partition, or when real EOF or error happened as signal.
            if query_headers.is_error():
                log_info(f"Query {ide} type: {type} config len:{len(configs)}, Error: {query_headers.get_error_code()}")
                self.type_expander.propagate_signal_in(query_headers)

                for config in configs:
                    joiner_id = f"{ide}_{config.join_id}"
                    joiner = self.joiners.pop(joiner_id, None)
                    if joiner: # Cleanup
                        self.state_storage.backup_query_final(joiner_id, joiner.version_id , joiner) # For debugging/final reviewing purposes
                        self.state_storage.unregister_query(joiner_id)
                    else:
                        log_info(f"Join acc {joiner_id}, cancelled but had no state.")

                # For multi config one do not batch logic for now. Since only ones with that are of 1 message len.
                return (joiner_id, batch_actions.FINISH_ACTION) if len(configs) == 1 else None

            log_info(f"Query {ide} type: {type} config len:{len(configs)}, EOF received")

            for config in configs:
                joiner_id = f"{ide}_{config.join_id}"
                joiner = self.joiners.get(joiner_id, None)

                if joiner == None:
                    log_info(f"Join acc {joiner_id}, EOF was the first message to be received")
                    joiner = JoinAccumulator(config, config.new_builder_for(query_headers), ide = joiner_id, limit = self.msg_rows_limit)
                    self.joiners[joiner_id] = joiner
                    self.state_storage.register_query(joiner_id, joiner)
                
                if config.left_type == type:
                    if joiner.handle_eof_left(query_headers.packet_id): 
                        self.state_storage.backup_query_final(joiner_id, joiner.version_id , joiner) # For debugging/final reviewing purposes
                        log_info(f"Join acc {joiner_id}, handling done, freeing. EOF Left was last message")
                        del self.joiners[joiner_id]
                        self.state_storage.unregister_query(joiner_id)                        
                    else:
                        self.state_storage.write_changes(joiner_id, joiner.version_id, joiner) # Save state
                        self.state_storage.commit_changes(joiner_id)
                        self.state_storage.push_changes(joiner_id, joiner.version_id, joiner, joiner.batch_ver_count)                    
                        joiner.batch_ver_count = 0

                elif joiner.handle_eof_right(query_headers.packet_id): # type == right_type, and eof 
                    self.state_storage.backup_query_final(joiner_id, joiner.version_id , joiner) # For debugging/final reviewing purposes

                    log_info(f"Join acc {joiner_id}, handling done, freeing. EOF Right was last message")
                    del self.joiners[joiner_id]
                    self.state_storage.unregister_query(joiner_id)

                else: # EOF handled but not actual EOF so just save the state for the config even If not on batch
                    self.state_storage.write_changes(joiner_id, joiner.version_id, joiner) # Save state
                    self.state_storage.commit_changes(joiner_id)
                    self.state_storage.push_changes(joiner_id, joiner.version_id, joiner, joiner.batch_ver_count)                    
                    joiner.batch_ver_count = 0
            
            if len(configs) > 1: # For multi config one do not batch logic for now. Since only ones with that are of 1 message len.
                return None

            # Not the best? but happens once per query so lets not dwell on it... 
            # If it was freed on the joiners we know If finished, else EOF was not last message

            #Python actually keeps the var in scope lol.
            if joiner_id in self.joiners:
                return (joiner_id, batch_actions.ACK_ACTION) # Ack even If not on batch size count!

            return (joiner_id, batch_actions.FINISH_ACTION)




        outputs = []
        row_actions= []

        for config in configs:
            joiner_id=f"{ide}_{config.join_id}"
            joiner = self.joiners.get(joiner_id, None)

            if joiner == None:
                log_info(f"Join acc {joiner_id}, Initing accumulator")
                joiner = JoinAccumulator(config, config.new_builder_for(query_headers), ide = joiner_id, limit = self.msg_rows_limit)
                self.joiners[joiner_id] = joiner
                self.state_storage.register_query(joiner_id, joiner)

            # Should check IF packet id is dupped and not add it IF it is.
            action= joiner.get_action_for_type(query_headers.packet_id, type)
            if action == None: # Duplicated! or invalid and so on.
                log_info(f"Join acc {joiner_id}, received duplicate packet {query_headers.packet_id}")
                continue

            outputs.append(joiner)
            # Given righ finished/left finished get actual action for config
            row_actions.append(action)


        if len(outputs) == 0: 
            log_info(f"Query {ide} type: {type} config len:{len(configs)}, duplicate message on all configs.")
            return # ack this message as stand alone

        msg = self.payload_deserializer(msg) 

        for row in msg.stream_rows():
            for action in row_actions:
                action(row)


        # Now If multi... do not batch logic
        if len(configs) > 1: # We still need to check config len not outputs, since dup logic could distort things
            for joiner in outputs:
                if joiner.add_check_msg_for_type(type, query_headers.packet_id):
                    joiner_id = joiner.joiner_id
                    self.state_storage.backup_query_final(joiner_id, joiner.version_id , joiner) # For debugging/final reviewing purposes

                    log_info(f"Join acc {joiner_id}, handling done, freeing. Final was payload msg")
                    del self.joiners[joiner_id]
                    self.state_storage.unregister_query(joiner_id)                        
                
                else: 
                    # Save changes.                    
                    self.state_storage.write_changes(joiner.joiner_id, joiner.version_id, joiner) # Save state
                    self.state_storage.commit_changes(joiner.joiner_id)
                    self.state_storage.push_changes(joiner.joiner_id, joiner.version_id, joiner, joiner.batch_ver_count)
                    joiner.batch_ver_count = 0

            return # No batch ack logic.. as stand alone


        # Just one joiner.
        joiner = outputs[0] # Not needed really since python saves the joiner on for but well.
        joiner_id = joiner.joiner_id

        if joiner.add_check_msg_for_type(type, query_headers.packet_id):
            self.state_storage.backup_query_final(joiner_id, joiner.version_id , joiner) # For debugging/final reviewing purposes
            log_info(f"Join acc {joiner_id}, handling done, freeing. Final was payload msg")
            del self.joiners[joiner_id]
            self.state_storage.unregister_query(joiner_id)
            return (joiner_id, batch_actions.FINISH_ACTION)

        # Only save/push on batch size count...
        if joiner.batch_ver_count >= self.batch_size:
            # log_info(f"Join acc {joiner_id}, batch save... count versions {joiner.batch_ver_count} .. version_id:{joiner.version_id}")
            self.state_storage.write_changes(joiner.joiner_id, joiner.version_id, joiner) # Save state
            self.state_storage.commit_changes(joiner.joiner_id)
            self.state_storage.push_changes(joiner.joiner_id, joiner.version_id, joiner, joiner.batch_ver_count)
            joiner.batch_ver_count = 0

            return (joiner_id, batch_actions.ACK_ACTION)


        return (joiner_id, batch_actions.ACCUMULATE_ACTION)



    def start(self):
        # Even If there is no pending changes, load states of queries, to continue on memory and not load always from disk.
        for joiner_id, (version_id, state) in self.state_storage.load_states().items():
            log_info(f"At start restored joiner: {joiner_id}, last version:{version_id}")
            state.version_id = version_id # To have it in mind for future messages/handling

        self.state_storage.check_integrity()

        self.middleware.start_consuming(self.handle_task)

    def start_on(self, middleware):
        middleware.start_consuming(self.handle_task)

    def close(self):
        self.middleware.close()
        self.type_expander.close()
