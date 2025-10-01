import itertools


GREATER_THAN_OP = ">"
GREATER_EQ_THAN_OP = ">="
LESSER_EQ_THAN_OP = "<="
LESSER_THAN_OP = "<"
BETWEEN_THAN_OP = "between"
EQUALS_ANY = "equals_any"
NOT_EQUALS = "not_equals"

class GreaterThanOP:
    def __call__(self, vl, constraints):
        return float(vl) > constraints[0]
    def filter_expression(self,field_key, vl, constraints):
        return (f"float({vl}) > {constraints[0]}",)
class GreaterEQThanOP:
    def __call__(self, vl, constraints):
        return float(vl) >= constraints[0]
    def filter_expression(self,field_key, vl, constraints):
        return (f"float({vl}) >= {constraints[0]}",)

class LesserThanOP:
    def __call__(self, vl, constraints):
        return float(vl) < constraints[0]
    def filter_expression(self,field_key, vl, constraints):
        return (f"float({vl}) < {constraints[0]}",)
class LesserEQThanOP:
    def __call__(self, vl, constraints):
        return float(vl) <= constraints[0]
    def filter_expression(self,field_key, vl, constraints):
        return (f"float({vl}) <= {constraints[0]}",)

class BetweenThanOP:
    def __call__(self, vl, constraints):
        return constraints[0]<= float(vl) <= constraints[1]
    def filter_expression(self,field_key, vl, constraints):
        return (f"{constraints[0]}<= float({vl}) <={constraints[1]}",)

class EqualsAnyOP:
    def __call__(self, vl, constraints):
        return any(vl == constraint for constraint in constraints)
    def filter_expression(self,field_key, vl, constraints):
        return (f"{vl} in self.equals_any_constraints_{field_key}",#f"any({vl} == constraint for constraint in self.equals_any_constraints)",
            (f"equals_any_constraints_{field_key}", f"set({constraints})"),
            )

class NotEqualsOP:
    def __call__(self, vl, constraints):
        return all(vl != constraint for constraint in constraints)

    def filter_expression(self,field_key, vl, constraints):
        return (f"all({vl} != constraint for constraint in self.not_equals_constraints_{field_key})",
            (f"not_equals_constraints_{field_key}", f"{constraints}")
            )


NUMBER_OPS = {
    GREATER_THAN_OP: GreaterThanOP,
    GREATER_EQ_THAN_OP: GreaterEQThanOP,
    LESSER_THAN_OP: LesserThanOP,
    LESSER_EQ_THAN_OP: LesserEQThanOP,
    BETWEEN_THAN_OP:BetweenThanOP,
}

GENERAL_OPS = {
    EQUALS_ANY: EqualsAnyOP,
    NOT_EQUALS: NotEqualsOP,
}


class RowFilter:
    def __init__(self, field_key, op_name, constraints):
        self.field_key = field_key
        self.constraints = constraints
        self.op = GENERAL_OPS.get(op_name, None)
        if not self.op:
            self.op = NUMBER_OPS.get(op_name, None)

        # Assert not None OP!
        assert self.op
        self.op = self.op()

    def should_keep(self, row):
        vl = row[self.field_key]
        return self.op(vl, self.constraints)

    def parse_expression(self):
        return self.op.filter_expression(str(self.field_key), f"row[{self.field_key}]", self.constraints)
    def parse_expression_str_field(self):
        return self.op.filter_expression(str(self.field_key), f"row['{self.field_key}']", self.constraints)


FILTER_FIELD_NAME = 0
FILTER_FIELD_OP = 1
FILTER_FIELD_VLS = 2


# filters serial should be [["fieldTarget", "operation", ["constraint1", "constraint2"]], ["fieldTarget", "operation", ["constraint1", "constraint2"]]]
# rows are {"field":vl, "field2":vl2}
def load_all_filters(filters_serial):
    all_filters = []
    for filt in filters_serial:
        all_filters.append(
            RowFilter(
                filt[FILTER_FIELD_NAME], filt[FILTER_FIELD_OP], filt[FILTER_FIELD_VLS]
            )
        )
    return all_filters

def parse_filters(filters):
    if len(filters) == 0:
        return ("", "True")

    res = [filt.parse_expression() for filt in filters]
    all_conditions = " and ".join([itm[1] for itm in res])
    all_states = "\n".join([f"self.{itm[0]} = {itm[1]}" for itm in res])

    return (all_states, all_conditions)
def parse_filters_str(filters):
    if len(filters) == 0:
        return ("", "True")

    res = [filt.parse_expression_str_field() for filt in filters]
    all_conditions = " and ".join([itm[0] for itm in res])
    all_states = "\n\t\t".join([f"self.{itm[0]} = {itm[1]}" for itm in itertools.chain.from_iterable(
        map(lambda x: x[1:],filter(lambda x: len(x) > 1, res))
    )])

    return (all_states, all_conditions)

def should_keep(filters, row):
    return all(filt.should_keep(row) for filt in filters)


def get_filter_source(filter_name, filters):
    expression= parse_filters_str(filters)
    source = f"""
class {filter_name}:
\tdef __init__(self):
\t\t{expression[0] if expression[0] != "" else "pass"}

\tdef should_keep(self, row):
\t\treturn {expression[1]}
\tdef should_keep_any(self, rows):
\t\treturn filter(self.should_keep, rows)
"""
    return source

def build_filter(filters_to_compile):
    source = get_filter_source("CompiledFilter",filters_to_compile)
    #print("Creating filter\n",source)
    local_ns = {}
    exec(compile(source, '<string>', 'exec'), local_ns)
    return local_ns["CompiledFilter"]()



def build_filter_from_config(filters_config):
    return build_filter(load_all_filters(filters_config))
