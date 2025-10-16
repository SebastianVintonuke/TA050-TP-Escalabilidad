"""
import heapq



def maintain_top_k(batch, K, key1, key2):
	heap = []
	for row in batch:
		key = (row[key1], -row[key2])
		if len(heap) < K:
			heapq.heappush(heap, (key, row))
		else:
			heapq.heappushpop(heap, (key, row))
	return [item[1] for item in sorted(heap, key=lambda x: (-x[0][0], -x[0][1]))]
	#return [heapq.heappop(heap)[1] for i in range(len(heap))]


class KeepKHeap:
	def __init__(self, limit, comp_key, comp_key2):
		self.limit = limit
		self.comp_key = comp_key
		self.comp_key2 = comp_key2
		self.ind = 0

	def add_new(self, heap, row):
		sort_key = (row[self.comp_key], -row[self.comp_key2])
		heap_item = (sort_key, row)
		heapq.heappush(heap, heap_item)		
	
	def add_to(self, heap, row):
		key = (row[self.comp_key], -row[self.comp_key2])
		if len(heap) < self.limit:
			heapq.heappush(heap, (key, self.ind, row))
			self.ind +=1
		else:
			heapq.heappushpop(heap, (key, self.ind, row))
			self.ind +=1


	def finished(self, heap):
		return [item[2] for item in sorted(heap, key=lambda x: (-x[0][0], -x[0][1]))]
"""


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

	def add_all(self, current_grouped, rows):
		for row in rows:
			current_grouped.append(row)


class KeepTopAction:
	def __init__(self, comp_key):
		self.comp_key = comp_key

	def add_new(self, current_grouped, row):
		row[self.comp_key] = float(row[self.comp_key])  # Map to float assumed float.
		current_grouped.append(row)

	def add_to(self, current_grouped, row):
		row[self.comp_key] = float(row[self.comp_key])  # Map to float assumed float.
		# Assumed len of grouped has to be one, you wont call this method without new.
		if row[self.comp_key] > current_grouped[0][self.comp_key]:
			current_grouped[0] = row
	def add_all(self, current_grouped, rows):
		itm = max(rows, key=lambda r: r[self.comp_key])
		if current_grouped[0][self.comp_key] < itm[self.comp_key]:
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
		row[self.comp_key] = float(row[self.comp_key])  # Map to float assumed float.
		current_grouped.append(row)

	def add_to(self, current_grouped, row):
		row[self.comp_key] = float(row[self.comp_key])  # Map to float assumed float.
		#Current grouped is assumed ordered since this is ordering it.
		if len(current_grouped) >= self.limit:
			ind =self._binary_insert_ind(current_grouped, row)
			if ind < self.limit: # Only insert If its on the top K
				current_grouped.insert(ind, row)
				current_grouped.pop()
		else: # No concerns about limit/top k, add it wherever
			current_grouped.insert(self._binary_insert_ind(current_grouped, row)
				, row)

	def add_all(self, current_grouped, rows):
		for row in rows:
			self.add_to(current_grouped, row)

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


ASCENDING_ORDER = "ASC"
DESCENDING_ORDER = "DESC"

class KeepAscOrDescK:
	def __init__(self, limit, comp_key, comp_key2):
		self.limit = limit
		self.comp_key = comp_key
		self.comp_key2 = comp_key2

	def _for_insert_ind(self, current_grouped, row):
		key = (row[self.comp_key], -row[self.comp_key2])
		
		for ind, itm in enumerate(current_grouped):
			if key > (itm[self.comp_key], -itm[self.comp_key2]):
				return ind
		
		return len(current_grouped)

	def _binary_insert_ind(self, current_grouped, row):
		key = row[self.comp_key]
		key2 = row[self.comp_key2]
		# Binary search directly on current_grouped using row[self.comp_key]
		lo, hi = 0, len(current_grouped)
		while lo < hi:
			mid = (lo + hi) // 2
			curr = current_grouped[mid]
			if key < curr[self.comp_key] or (key == curr[self.comp_key] and key2 >= curr[self.comp_key2]) :
			    lo = mid + 1
			else:
			    hi = mid
		return lo


	def add_new(self, current_grouped, row):
		row[self.comp_key] = float(row[self.comp_key])  # Map to float assumed float.
		row[self.comp_key2] = int(row[self.comp_key2])		
		current_grouped.append(row)

	def add_to(self, current_grouped, row):
		row[self.comp_key] = float(row[self.comp_key])  # Map to float assumed float.
		row[self.comp_key2] = int(row[self.comp_key2])

		#Current grouped is assumed ordered since this is ordering it.
		if len(current_grouped) >= self.limit:
			ind =self._binary_insert_ind(current_grouped, row)
			if ind < self.limit: # Only insert If its on the top K
				current_grouped.insert(ind, row)
				current_grouped.pop()
		else: # No concerns about limit/top k, add it wherever
			current_grouped.insert(self._binary_insert_ind(current_grouped, row)
				, row)

	def add_all(self, current_grouped, rows):
		for row in rows:
			key = row[self.comp_key]
			key2 = row[self.comp_key2]
			# Binary search directly on current_grouped using row[self.comp_key]
			lo, hi = 0, len(current_grouped)
			while lo < hi:
				mid = (lo + hi) // 2
				curr = current_grouped[mid]
				if key < curr[self.comp_key] or (key == curr[self.comp_key] and key2 >= curr[self.comp_key2]) :
				    lo = mid + 1
				else:
				    hi = mid
			if len(current_grouped) >= self.limit:
				if lo < self.limit: # Only insert If its on the top K
					current_grouped.insert(lo, row)
					current_grouped.pop()
			else: # No concerns about limit/top k, add it wherever
				current_grouped.insert(lo, row)


KEEP_ASCDESC= "KEEP_ASCDESC"
ROW_ACTIONS = {
	KEEP_ALL_ROWS: KeepAllRowsAction,
	KEEP_TOP_K: KeepTopKAction,
	KEEP_LEAST_K: KeepLeastKAction,
	KEEP_TOP: KeepTopAction,
	KEEP_ASCDESC: KeepAscOrDescK,
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

	def add_all_group_acc(self, acc, rows):
		self.grouper.add_all(acc, rows)

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
