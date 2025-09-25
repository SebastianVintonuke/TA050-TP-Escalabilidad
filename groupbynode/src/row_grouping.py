

SUM_ACTION = "sum"
AVG_ACTION = "avg"
MAX_ACTION = "max"
COUNT_ACTION = "count"

class CountAction:
	def new(self, value): # By default it doesnt do anything? just return 1
		return 1 

	def add_value(self, acc, value): # Ignore value completely.. just add to it
		return acc+1

class SumAction:
	def new(self, value):
		return value
	def add_value(self, acc, value):
		return acc+value

class MaxAction:
	def new(self, value):
		return value

	def add_value(self, acc, value):
		return value if value > acc else acc


class AvgAction:
	def new(self, value):
		return (1, float(value)) #(count , curr_avg)
	def add_value(self, acc, value):
		factor = (float(acc[0]))/acc[0]+1 # (n/n+1)
		acc[1]+= value*factor # curr_avg + value*(n/n+1)
		acc[0]+=1 # count+=1
		return acc




NUMBER_ACTIONS = {
	SUM_ACTION: SumAction,
	MAX_ACTION: MaxAction,
	AVG_ACTION: AvgAction,
	COUNT_ACTION: CountAction,
}


def resolve_action(op_name):
	creator = NUMBER_ACTIONS.get(op_name, None)

	# Assert not None creator!
	assert creator

	return creator() # Create creator.

class RowGrouper:
	def __init__(self, fields_key, group_actions):
		self.fields_key = fields_key
		self.group_actions = {}
		
		for col, action in group_actions.items():
			self.group_actions[col] = resolve_action(action)


	def new_group_acc(self, row):
		acc ={} 
		for key,action in self.group_actions.items():
			acc[key] = action.new(row[key])

		return acc

	def add_group_acc(self, acc, row):
		for key,action in self.group_actions.items():
			acc[key] = action.add_value(acc[key], row[key])


FILTER_FIELDS_NAME = 0
FILTER_FIELD_ACTIONS = 1

# filters serial should be [["fieldTarget", "operation", ["constraint1", "constraint2"]], ["fieldTarget", "operation", ["constraint1", "constraint2"]]]
# rows are {"field":vl, "field2":vl2}
def load_groupers(groupers_serial):
	all_groupers = []
	for grouper in groupers_serial:
		all_groupers.append(RowGrouper(grouper[FILTER_FIELDS_NAME],
			grouper[FILTER_FIELD_ACTIONS]))
	return all_groupers
