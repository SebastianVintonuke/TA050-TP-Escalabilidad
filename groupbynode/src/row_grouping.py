KEEP_ALL_ROWS = "keep_all_rows"
KEEP_TOP_K = "keep_top_k"
KEEP_LEAST_K = "keep_least_k"
KEEP_TOP = "keep_top_row"

### ROW ACTIONS
class KeepAllRowsAction:
	def __init__(self):
		pass

	def add_new(self, current_grouped, row):
		current_grouped.append(row)

	def add_to(self, current_grouped, row):
		current_grouped.append(row)

class KeepTopAction:
	def __init__(self, comp_key):
		self.comp_key = comp_key

	def add_new(self, current_grouped, row):
		current_grouped.append(row)

	def add_to(self, current_grouped, row):
		# Assumed len of grouped has to be one, you wont call this method without new.
		if row[self.comp_key] > current_grouped[0][self.comp_key]:
			current_grouped[0] = row

class KeepTopKAction:
	def __init__(self, comp_key, limit):
		self.comp_key = comp_key
		self.limit = limit

	def _binary_insert_ind(self, current_grouped, row):
		key = row[self.comp_key]
		# Binary search directly on current_grouped using row[self.comp_key]
		lo, hi = 0, len(current_grouped)
		while lo < hi:
			mid = (lo + hi) // 2
			if key <= current_grouped[mid][self.comp_key] :
			    lo = mid + 1
			else:
			    hi = mid
		return lo

	def add_new(self, current_grouped, row):
		current_grouped.append(row)

	def add_to(self, current_grouped, row):
		#Current grouped is assumed ordered since this is ordering it.
		if len(current_grouped) >= self.limit:
			ind =self._binary_insert_ind(current_grouped, row)
			if ind < self.limit: # Only insert If its on the top K
				current_grouped.insert(ind, row)
				current_grouped.pop()
		else: # No concerns about limit/top k, add it wherever
			current_grouped.insert(self._binary_insert_ind(current_grouped, row)
				, row)


class KeepLeastKAction(KeepTopKAction):
	def __init__(self, comp_key, limit):
		super().__init__(comp_key, limit)

	def _binary_insert_ind(self, current_grouped, row):
		key = row[self.comp_key]
		lo, hi = 0, len(current_grouped)
		# Binary search directly on current_grouped using row[self.comp_key]
		while lo < hi:
			mid = (lo + hi) // 2
			if key >= current_grouped[mid][self.comp_key]: 
			    lo = mid + 1
			else:
			    hi = mid
		return lo

ROW_ACTIONS = {
	KEEP_ALL_ROWS: KeepAllRowsAction,
	KEEP_TOP_K: KeepTopKAction,
	KEEP_LEAST_K: KeepLeastKAction,
	KEEP_TOP: KeepTopAction,
}

class RowGrouper:
	def __init__(self, grouping_config):
		self.grouper = load_grouper_action(grouping_config)

	def new_group_acc(self, row):
		acc =[] # Acc is list of rows
		self.grouper.add_new(acc, row)
		return acc

	def add_group_acc(self, acc, row):
		self.grouper.add_to(acc, row)

	def iterate_rows(self, acc):
		return iter(acc) # acc is the list of rows basically

MAP_GROUP_ACTION = 0
MAP_ACTION_PARAMS = 1
#
## Eg: ["keep_all_rows", {}]
## Eg: ["keep_top_k", {"comp_col":"ganancia", "limit": 3}]
def load_grouper_action(grouper_serial):
	creator = ROW_ACTIONS.get(grouper_serial[MAP_GROUP_ACTION], None)
	assert creator != None
	return creator(**grouper_serial[MAP_ACTION_PARAMS])
