

SUM_ACTION = "sum"
AVG_ACTION = "avg"
MAX_ACTION = "max"
COUNT_ACTION = "count"

class CountAction:
	def new(self): # By default it doesnt do anything? just return 1
		return 1 

	def add_value(self, acc): # Ignore value completely.. just add to it
		return acc+1
	def get_result(self, acc):
		return acc


class SumAction:
	def new(self, value):
		return value
	def add_value(self, acc, value):
		return acc+value
	def get_result(self, acc):
		return acc

class MaxAction:
	def new(self, value):
		return value

	def add_value(self, acc, value):
		return value if value > acc else acc
	def get_result(self, acc):
		return acc


class AvgAction:
	def new(self, value):
		return [1, float(value)] #(count , curr_avg)
	def add_value(self, acc, value):
		factor = (float(acc[0]))/(acc[0]+1) # (n/n+1)
		value = float(value)/(acc[0]+1)

		acc[1]= acc[1] *factor + value # curr_avg + value*(n/n+1)
		acc[0]+=1 # count+=1
		return acc
	def get_result(self, acc):
		return acc[1]




NUMBER_ACTIONS = {
	SUM_ACTION: SumAction,
	MAX_ACTION: MaxAction,
	AVG_ACTION: AvgAction,
}

#ROW_ACTIONS = {
#	COUNT_ACTION: CountAction,
#}

#[fields, group_acc_actions]



def resolve_action(op_name):
	creator = NUMBER_ACTIONS.get(op_name, None)
	assert creator != None
	return creator() # Create creator.

class RowGrouper:
	def __init__(self, fields_key, group_actions):
		self.fields_key = fields_key
		self.group_actions = {}
		self.count_out_fields = []

		for col, action in group_actions.items():
			# Not the most efficient I guess..
			if action == COUNT_ACTION:
				self.count_out_fields.append(col)
				continue

			self.group_actions[col] = resolve_action(action)

	def get_group_key(self, row):
		key = []

		for field in self.fields_key:
			key.append(row[field])

		return tuple(key) 

	def new_group_acc(self, row):
		acc ={} 
		for field in self.count_out_fields:
			acc[field] = 1

		for key,action in self.group_actions.items():
			acc[key] = action.new(row[key])

		return acc

	def add_group_acc(self, acc, row):
		for field in self.count_out_fields:
			acc[field] += 1

		for key,action in self.group_actions.items():
			acc[key] = action.add_value(acc[key], row[key])


	# Expands to a list of rows in dict mode {}
	def expand_with_key(self, key, acc):
		base = {}
		ind = 0
		for field in self.fields_key:
			base[field] = key[ind]
			ind+=1

		for field in self.count_out_fields:
			base[field] = acc[field]
		
		for key,action in self.group_actions.items():
			base[key] = action.get_result(acc[key])

		return base



FILTER_FIELDS_NAME = 0
FILTER_FIELD_ACTIONS = 1

# filters serial should be [["fieldTarget", "operation", ["constraint1", "constraint2"]], ["fieldTarget", "operation", ["constraint1", "constraint2"]]]
# rows are {"field":vl, "field2":vl2}
def load_grouper(grouper_serial):
	return RowGrouper(grouper[FILTER_FIELDS_NAME], grouper[FILTER_FIELD_ACTIONS])

def load_groupers(groupers_serial):
	all_groupers = []
	for grouper in groupers_serial:
		all_groupers.append(RowGrouper(grouper[FILTER_FIELDS_NAME],
			grouper[FILTER_FIELD_ACTIONS]))
	return all_groupers
