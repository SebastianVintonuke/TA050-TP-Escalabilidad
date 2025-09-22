

NUMBER_OPS = {
	"<": lambda vl,constraints: int(vl) < int(constraints[0]),
	">": lambda vl,constraints: int(vl) > int(constraints[0]),
	"between": lambda vl,constraints: constraints[0] <= vl <= constraints[1],
}

GENERAL_OPS = {
	"==": lambda vl,constraints: any(vl== constraint for constraint in constraints),
	"!=": lambda vl,constraints: all(vl== constraint for constraint in constraints),
}


class RowFilter:
	def __init__(self, field_ind, op_name, constraints):
		self.field_ind = field_ind
		self.constraints = constraints
		self.op = self.GENERAL_OPS.get(
				op_name, None
			)
		if not self.op:
			self.op = self.NUMBER_OPS.get(
					op_name, None)

		# Assert not None OP!
		assert self.op

	def should_keep(self,row):
		vl = row[self.field_ind]
		return self.op(vl, self.constraints)



FILTER_FIELD_NAME = 0
FILTER_FIELD_OP = 1
FILTER_FIELD_VLS = 2

def load_all_filters(field_mapper, filters_serial):
	all_filters = []
	for filt in filters_serial:
		all_filters.append(RowFilter(field_mapper[filt[FILTER_FIELD_NAME]],
			filt[FILTER_FIELD_OP], 
			filt[FILTER_FIELD_VLS]
			))
	return all_filters