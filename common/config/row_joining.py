INNER_ON_EQ = "inner_on_eq"

class InnerEqualJoin:
    def __init__(self, col_left, col_right):
        self.col_left = col_left
        self.col_right = col_right

    def should_join(self, row1, row2):
        return row1[self.col_left] == row2[self.col_right]


JOINING_OPS = {
    INNER_ON_EQ: InnerEqualJoin,
}

JOIN_FIELD_OP = 0
JOIN_FIELD_ARGS = 1


def load_joiner(joiner_serial):
    creator = JOINING_OPS.get(joiner_serial[JOIN_FIELD_OP], None)
    assert creator != None
    return creator(**joiner_serial[JOIN_FIELD_ARGS])