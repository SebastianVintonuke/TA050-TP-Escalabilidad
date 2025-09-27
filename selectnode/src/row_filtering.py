

GREATER_THAN_OP = ">"
GREATER_EQ_THAN_OP = ">="
LESSER_EQ_THAN_OP = "<="
LESSER_THAN_OP = "<"
BETWEEN_THAN_OP = "between"
EQUALS_ANY = "equals_any"
NOT_EQUALS = "not_equals"

NUMBER_OPS = {
	GREATER_THAN_OP: lambda vl,constraints: float(vl) > constraints[0],
	GREATER_EQ_THAN_OP: lambda vl,constraints: float(vl) >= constraints[0],
	LESSER_THAN_OP: lambda vl,constraints: float(vl) < constraints[0],
	LESSER_EQ_THAN_OP: lambda vl,constraints: float(vl) <= constraints[0],
	BETWEEN_THAN_OP: lambda vl,constraints: constraints[0] <= float(vl) <= constraints[1],
}

GENERAL_OPS = {
	EQUALS_ANY: lambda vl,constraints: any(vl== constraint for constraint in constraints),
	NOT_EQUALS: lambda vl,constraints: all(vl!= constraint for constraint in constraints),
}

class RowFilter:
	def __init__(self, field_key, op_name, constraints):
		self.field_key = field_key
		self.constraints = constraints
		self.op = GENERAL_OPS.get(
				op_name, None
			)
		if not self.op:
			self.op = NUMBER_OPS.get(
					op_name, None)

		# Assert not None OP!
		assert self.op

	def should_keep(self,row):
		vl = row[self.field_key]
		return self.op(vl, self.constraints)



FILTER_FIELD_NAME = 0
FILTER_FIELD_OP = 1
FILTER_FIELD_VLS = 2

# filters serial should be [["fieldTarget", "operation", ["constraint1", "constraint2"]], ["fieldTarget", "operation", ["constraint1", "constraint2"]]]
# rows are {"field":vl, "field2":vl2}
def load_all_filters(filters_serial):
	all_filters = []
	for filt in filters_serial:
		all_filters.append(RowFilter(filt[FILTER_FIELD_NAME],
			filt[FILTER_FIELD_OP], 
			filt[FILTER_FIELD_VLS]
			))
	return all_filters


def should_keep(filters, row):
	return all(filt.should_keep(row) for filt in filters)