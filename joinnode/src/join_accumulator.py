from common.state_storage.packet_id_tracker import PacketIDTracker

import logging
# log_info = print
log_info = logging.info

DEFAULT_LIMIT= 10000
class JoinAccumulator:
    def __init__(self, type_conf, msg_builder, limit = DEFAULT_LIMIT, ide= ""):
        self.type_conf = type_conf
        self.msg_builder = msg_builder#type_conf.new_builder_for(msg, ind)
        self.msg_builder.reset_eof()# Ensure its not copying the eof flag from input sender

        # Also reset packet id to 0... output packet id thing. If restored from state this starting id 
        # is changed.
        self.msg_builder.headers.packet_id = 0 

        self.left_rows = []
        self.right_rows = []
        self.limit = limit


        self.left_packet_tracker =PacketIDTracker()
        self.left_last_packet_id = -1
        self.left_finished = False

        self.right_packet_tracker =PacketIDTracker()
        self.right_last_packet_id = -1
        self.right_finished = False

        self.joiner_id = ide#ide+ self.type_conf.join_id

        self.batch_ver_count = 0
        self.version_id = 0

    def reload_status(self):
        # log_info(f"AT reload ... left status? last pck: {self.left_last_packet_id}. exp new:{self.left_packet_tracker.expected_next_packet} .. missing?: {self.left_packet_tracker.missing_packets}")

        self.left_finished = self.left_last_packet_id >=0 and self.left_packet_tracker.handled_all_up_to(self.left_last_packet_id)
        self.right_finished = self.right_last_packet_id >=0 and self.right_packet_tracker.handled_all_up_to(self.right_last_packet_id)

    def add_result_rows(self, rows):
        for row in rows:
            self.msg_builder.add_raw_row(row)

    def set_result_rows(self, rows):
        self.msg_builder.clear_payload()
        for row in rows:
            self.msg_builder.add_raw_row(row)

    def get_out_rows(self):
        return self.msg_builder.payload


    def get_partial_result(self):
        out = self.msg_builder.clone()

        out.payload = [itm for itm in self.msg_builder.payload]
        return out

    def get_curr_out_id(self):
        return self.msg_builder.headers.packet_id

    def set_curr_out_id(self, pck_id):
        self.msg_builder.headers.packet_id = pck_id


    def len_left(self):
        return len(self.left_rows)

    def len_right(self):
        return len(self.right_rows)

    def len_joined(self):
        return self.msg_builder.len_payload()

    def add_joined(self , row):

        # print("--------->ADDING OUT ROW!", row)
        self.msg_builder.add_row(row)
        if self.msg_builder.len_payload() >= self.limit:
            # print("--------->SENDING PAYLOAD!", self.msg_builder.len_payload() , ">=", self.limit)
            self.type_conf.send(self.msg_builder)
            self.msg_builder.clear_payload()

            self.msg_builder.headers.packet_id += 1            

    def _trigger_eof_right(self):
        self.right_finished = True
        log_info(f"Join acc: {self.joiner_id} triggered RIGHT EOF left finished? {self.left_finished}")

        if self.left_finished:
            self.send_eof()
            return True
        
        # Not finished, then try join all left rows.
        for left_row in self.left_rows:
            self.type_conf.do_join_left_row(self.right_rows, left_row, self.add_joined)
        
        self.left_rows = [] # Empty it since already used.
        return False


    def _trigger_eof_left(self):
        self.left_finished = True
        log_info(f"Join acc: {self.joiner_id} triggered LEFT EOF {self.type_conf.left_type} right finished? {self.right_finished}")

        if self.right_finished:
            self.send_eof()
            return True
        
        # Not finished, then try join all right rows.
        for right_row in self.right_rows:
            self.type_conf.do_join_right_row(self.left_rows, right_row, self.add_joined)

        self.right_rows = [] # Empty it since already used.            
        return False



    def get_action_for_type(self,packet_id, type):
        if type == self.type_conf.left_type:
            if self.left_packet_tracker.is_duplicate(packet_id):
                return None # Do nothing is a duplicated packet!


            # Left message actions
            if self.right_finished:
                return (self.do_join_left_row)
            return (self.add_row_left)

        if self.right_packet_tracker.is_duplicate(packet_id):
            return None # Do nothing is a duplicated packet!

        # Right message actions
        if self.left_finished:
            return (self.do_join_right_row)
        return (self.add_row_right)

    def get_actions_for_type(self,type):
        if type == self.type_conf.left_type:
            # Left message actions
            if self.right_finished:
                return (self.add_check_msg_left, self.do_join_left_row)
            return (self.add_check_msg_left, self.add_row_left)
        # Right message actions
        if self.left_finished:
            return (self.add_check_msg_right, self.do_join_right_row)
        return (self.add_check_msg_right, self.add_row_right)


    def add_check_msg_for_type(self, type, packet_id):
        if type == self.type_conf.left_type:
            return self.add_check_msg_left(packet_id)
        return self.add_check_msg_right(packet_id)

    def add_check_msg_left(self, packet_id):
        self.version_id += 1 # Inc version by one each msg be it left or right.
        self.batch_ver_count += 1

        self.left_packet_tracker.check_new_packet(packet_id)

        left_finished = self.left_last_packet_id>=0 and self.left_packet_tracker.handled_all_up_to(self.left_last_packet_id)

        if left_finished:
            log_info(f"Join {self.type_conf.join_id} received final left msg {packet_id}, eof packet {self.left_last_packet_id}")
            return self._trigger_eof_left()

        return False

    def add_check_msg_right(self, packet_id):
        self.version_id += 1 # Inc version by one each msg be it left or right.
        self.batch_ver_count += 1

        self.right_packet_tracker.check_new_packet(packet_id)

        right_finished = self.right_last_packet_id>=0 and self.right_packet_tracker.handled_all_up_to(self.right_last_packet_id)

        if right_finished:
            log_info(f"Join {self.type_conf.join_id} received final right msg {packet_id}, eof packet {self.right_last_packet_id}")
            return self._trigger_eof_right()
            
        return False

    def handle_eof_left(self, eof_packet_id):
        if self.left_finished: # Check for duplicates.. If already finished then it would be one.
            return self._trigger_eof_left() # Do not add version id or so. But why not check anyway 

        self.left_last_packet_id = eof_packet_id
        self.left_packet_tracker.check_new_packet(eof_packet_id)
        self.version_id += 1 # Inc version by one each msg be it left or right.
        self.batch_ver_count += 1

        if self.left_packet_tracker.handled_all_up_to(eof_packet_id):
            return self._trigger_eof_left()

        log_info(f"Join {self.type_conf.join_id} received eof left without receiving all messages, missing: {len(self.left_packet_tracker.missing_packets)}")
        return False


    def handle_eof_right(self, eof_packet_id):
        if self.right_finished: # Check for duplicates.. If already finished then it would be one.
            return self._trigger_eof_right() # Do not add version id or so. But why not check anyway 

        self.right_last_packet_id = eof_packet_id
        self.right_packet_tracker.check_new_packet(eof_packet_id)
        self.version_id += 1 # Inc version by one each msg be it left or right.
        self.batch_ver_count += 1

        if self.right_packet_tracker.handled_all_up_to(eof_packet_id):
            return self._trigger_eof_right()
            
        log_info(f"Join {self.type_conf.join_id} received eof right without receiving all messages, missing: {len(self.right_packet_tracker.missing_packets)}")
        return False


    def do_join_right_row(self, right_row):
        self.type_conf.do_join_right_row(self.left_rows, right_row, self.add_joined)
        #self.describe()

    def do_join_left_row(self, left_row):
        self.type_conf.do_join_left_row(self.right_rows, left_row, self.add_joined)
        #self.describe()

    def add_row_left(self, left_row):
        self.left_rows.append(left_row)
        #self.describe()

    def add_row_right(self, right_row):
        self.right_rows.append(right_row)
        #self.describe()




    def send_eof(self): # What happens If the groupbynode fails here/shutdowns here?
        if self.msg_builder.has_payload(): # if it has payload send it
            self.describe_send()
            self.type_conf.send(self.msg_builder)
            
        eof_signal = self.msg_builder.clone()
        eof_signal.headers.packet_id+=1
        eof_signal.set_as_eof()

        log_info(f"{self.joiner_id} Send EOF {eof_signal.headers}")
        
        self.type_conf.send(eof_signal)

    def describe_send(self):
        log_info(f"SENDING TO {self.msg_builder.headers.types} {self.msg_builder.headers.ids}")
        self.describe()
        #for itm in self.msg_builder.payload:
        #    log_info(f"ROW {itm}")

    def describe(self):
        log_info(f"curr status join finished left:{self.left_finished} right: {self.right_finished}")
        log_info(f"row len left:{len(self.left_rows)} right: {len(self.right_rows)}")
        log_info(f"joined payload len:{self.msg_builder.len_payload()}")