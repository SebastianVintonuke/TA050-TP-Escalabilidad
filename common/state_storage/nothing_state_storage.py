


## Just define the contract of state storage..
class NothingQueryStateStorage:
	def __init__(self, manager):
		pass

	## This loads the states and removes all except highest query_state
	def load_states(self):
		return {}

	# -------------------------------------------------------------
	#				   Defined design/contract
	# -------------------------------------------------------------

	def check_integrity(self):
		return {}

	# -------------------------------------------------------------
	# 1. register_query
	# -------------------------------------------------------------

	def register_query(self, query_id, metadata, initial_packet_id = 0):
		pass

    def is_query_registered(self, query_id):
        return True
	# -------------------------------------------------------------
	# 5. unregister_query
	# -------------------------------------------------------------
		
	def unregister_query(self, query_id):
		pass


    def cancel_query(self, query_id):
    	pass

    def is_cancelled_query(self, query_id):
        return False


    ## For debugging purposes and so on
    def backup_query(self, query_id):
    	pass


	# -------------------------------------------------------------
	# 2. add_changes
	# -------------------------------------------------------------

	def write_changes(self, query_id, packet_id, changes):
		pass

	# -------------------------------------------------------------
	# 3. commit_changes
	# -------------------------------------------------------------

	def commit_changes(self, query_id):
		pass

	# -------------------------------------------------------------
	# 4. push_changes
	## Lets assume caller has the changes saved on change_file... change file is just for backup in case of a crash.
	## And also has prev state since we assume non concurrent modifying 
	## SOO essentially received the new state calculated from get new state
	# -------------------------------------------------------------
	def push_changes(self, query_id, packet_id, new_state, count_msgs): ## Lets 
		pass


	def set_ack_tags(self, tags):
		self.acks = ack_tags

	def ack_finished(self, ack_func):
		if self.acks:		
			for ack in self.acks:
				ack_func(ack)
			self.acks = []

	def unregister_packet(self, tag):
		pass