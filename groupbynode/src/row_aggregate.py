

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
		return float(value)
	def add_value(self, acc, value):
		return acc+float(value)
	def get_result(self, acc):
		return acc

class MaxAction:
	def new(self, value):
		return float(value)

	def add_value(self, acc, value):
		value = float(value)
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



#[fields, group_acc_actions]

def resolve_basic_aggregate_action(op_name):
	creator = NUMBER_ACTIONS.get(op_name, None)
	assert creator != None
	return creator() # Create creator.

ACTION_COL_IN = 1
ACTION_COL_OUT = 2
ACTION_OP = 0
class RowAggregator:
	def __init__(self, group_actions):
		self.group_actions = []
		self.count_out_fields = []

		for action in group_actions:
			# Not the most efficient I guess..
			if action[ACTION_OP] == COUNT_ACTION:
				self.count_out_fields.append(action[ACTION_COL_IN]) # For count action col in is the out col. Since no col out needed
				continue

			self.group_actions.append(( action[ACTION_COL_IN], 
										resolve_basic_aggregate_action(action[ACTION_OP]),
										action[ACTION_COL_OUT] if len(action)>2 else action[ACTION_COL_IN]
										))

	def new_group_acc(self, row):
		acc ={} 
		for field in self.count_out_fields:
			acc[field] = 1

		for col_in,action, col_out in self.group_actions:
			acc[col_out] = action.new(row[col_in])

		return acc

	def add_group_acc(self, acc, row):
		for field in self.count_out_fields:
			acc[field] += 1

		for col_in,action,col_out in self.group_actions:
			acc[col_out] = action.add_value(acc[col_out], row[col_in])


	# Expands to a list of rows in dict mode {}
	def add_aggregated_to(self, base, acc):
		for field in self.count_out_fields:
			base[field] = acc[field]
		for col_in,action, col_out in self.group_actions:
			base[col_out] = action.get_result(acc[col_out])

	# Expands to a list of rows, this only throws one row, the aggregate result, but its better if it has the same contract as row grouping
	def iterate_rows(self, acc):
		return iter([acc]) # acc is the list of rows basically