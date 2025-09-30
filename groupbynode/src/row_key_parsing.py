### Simple key parsing
class KeyGroupParser:
	def __init__(self, fields_key):
		self.fields_key = fields_key

	def get_group_key(self, row):
		key = []
		
		for field in self.fields_key:
			key.append(row[field])

		return tuple(key) 

	def get_base_key(self, key):
		base = {}
		ind = 0
		for field in self.fields_key:
			base[field] = key[ind]
			ind+=1

		return base
