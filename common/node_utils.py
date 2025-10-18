
class RestartLogic:
    def __init__(self, allowed_type):
        self.restart= True
        self.allowed_type = allowed_type

	def start_node_loop(self, node):
        while self.restart:
            try:
                node.start()
                self.stop_restart()
            except self.allowed_type as e:
                logging.error(f"Non fatal fail {e}")

	def start_node_loop_verbose(self, node):
		import traceback # Import it only If verbose, start node loop is called only once. So its fine

        while self.restart:
            try:
                node.start()
                self.stop_restart()
            except self.allowed_type as e:
                traceback.print_exc()
                logging.error(f"Non fatal fail {e}")

	def stop_restart(self):
		self.restart = False
