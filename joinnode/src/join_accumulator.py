
import logging
DEFAULT_LIMIT= 10000
class JoinAccumulator:
    def __init__(self, type_conf, msg, ind, limit = DEFAULT_LIMIT):
        self.type_conf = type_conf
        self.msg_builder = type_conf.new_builder_for(msg, ind)

        self.left_rows = []
        self.right_rows = []
        self.left_finished = False
        self.right_finished = False
        self.limit = limit

    def len_left(self):
        return len(self.left_rows)

    def len_right(self):
        return len(self.right_rows)

    def len_joined(self):
        return self.msg_builder.len_payload()

    def add_joined(self , row):
        self.msg_builder.add_row(row)
        if self.msg_builder.len_payload() >= self.limit:
            self.type_conf.send(self.msg_builder)
            self.msg_builder.clear_payload()

    def handle_eof_left(self):
        self.left_finished = True
        logging.info(f"HANDLING EOF LEFT {self.type_conf.left_type} out types: {self.msg_builder.types} right finished? {self.right_finished}")

        if self.right_finished:
            self.send_eof()
            return True
        
        # Not finished, then try join all right rows.
        for right_row in self.right_rows:
            self.type_conf.do_join_right_row(self.left_rows, right_row, self.add_joined)

        self.right_rows = [] # Empty it since already used.            
        return False
        #self.describe()


    def handle_eof_right(self):
        self.right_finished = True
        logging.info(f"HANDLING EOF RIGHT out types: {self.msg_builder.types} left finished? {self.left_finished}")
        if self.left_finished:
            self.send_eof()
            return True
        
        # Not finished, then try join all left rows.
        for left_row in self.left_rows:
            self.type_conf.do_join_left_row(self.right_rows, left_row, self.add_joined)
        
        self.left_rows = [] # Empty it since already used.
        return False
        #self.describe()

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

    def get_action_for_type(self,type):
        if type == self.type_conf.left_type:
            # Left message actions
            if self.right_finished:
                return self.do_join_left_row
            return self.add_row_left
        # Right message actions
        if self.left_finished:
            return self.do_join_right_row
        return self.add_row_right




    def send_eof(self): # What happens If the groupbynode fails here/shutdowns here?
        if self.msg_builder.has_payload(): # if it has payload send it
            self.describe_send()
            self.type_conf.send(self.msg_builder)

        eof_signal = self.msg_builder.clone()
        logging.info(f"EOF SIGNAL TO {eof_signal.types} {eof_signal.ids}")
        eof_signal.set_as_eof()
        self.type_conf.send(eof_signal)
        eof_signal.set_finish_signal()
        self.type_conf.send(eof_signal)

    def describe_send(self):
        logging.info(f"SENDING TO {self.msg_builder.types} {self.msg_builder.ids}")
        self.describe()
        for itm in self.msg_builder.payload:
            logging.info(f"ROW {itm}")

    def describe(self):
        logging.info(f"curr status join finished left:{self.left_finished} right: {self.right_finished}")
        logging.info(f"row len left:{len(self.left_rows)} right: {len(self.right_rows)}")
        logging.info(f"joined payload len:{self.msg_builder.len_payload()}")