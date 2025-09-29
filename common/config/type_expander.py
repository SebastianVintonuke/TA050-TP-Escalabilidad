import logging

class TypeExpander:
	def __init__(self):
		self.type_configurations = []
		self.expansions = {}

	def add_configurations(self, expansion, *args):
		expansions= []
		if expansion in  self.expansions:
			expansions = self.expansions[expansion]
		else:
			self.expansions[expansion] = expansions
		
		for configuration in args:
			self.type_configurations.append(configuration)
			expansions.append(configuration)

	def add_configuration_to_many(self, config, *args):
		self.type_configurations.append(config)
		for expansion in args:
			if expansion in  self.expansions:
				self.expansions[expansion].append(config)
			else:
				self.expansions[expansion] = [config]




	def from_dict(self, type_configs):
		for key, config in type_configs.items():
			self.add_configurations(key, config)

	def get_configurations_for(self, exp):
		return self.expansions[exp]

	def get_configuration_for(self, exp):
		return self.expansions[exp][0]

	def propagate_signal_in(self, msg):
		for ind in range(len(msg.types)):
			# Empty message that has same headers splitting to each destination.
			for config in self.get_configurations_for(msg.types[ind]):
				propagated_msg = config.new_builder_for(msg, ind)
				config.send(propagated_msg)

	def close(self):
		for config in self.type_configurations:
			try:
				config.close()
			except Exception as e:
				logging.error(f"Configuration close failed {e}")