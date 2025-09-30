INNER_ON_EQ = "inner_on_eq"

class InnerEqualJoin:
    def __init__(self, col_left, col_right):
        self.col_left = col_left
        self.col_right = col_right

    def should_join(self, row1, row2):
        return row1[self.col_left] == row2[self.col_right]

    def map_cols(self, left_mapper, right_mapper):
        self.col_left = left_mapper(self.col_left)
        self.col_right = right_mapper(self.col_right)

JOINING_OPS = {
    INNER_ON_EQ: InnerEqualJoin,
}

JOIN_FIELD_OP = 0
JOIN_FIELD_ARGS = 1


def load_joiner(joiner_serial):
    creator = JOINING_OPS.get(joiner_serial[JOIN_FIELD_OP], None)
    assert creator != None
    return creator(**joiner_serial[JOIN_FIELD_ARGS])


class JoinProjectMapperAll:
    def __call__(self, row_left, row_right):
        return row_left + row_right


class JoinProjectMapper:
    def __init__(self, left_cols, right_cols):
        self.left_cols = left_cols
        self.right_cols = right_cols

    def __call__(self, row_left, row_right):
        res = []
        for col in self.left_cols:
            res.append(str(row_left[col]))
        for col in self.right_cols:
            res.append(str(row_right[col]))
        return res


class JoinProjectMapperOrdered:
    ## COls received are in format [index, col], to keep the order
    def __init__(self, left_cols, right_cols):
        self.left_cols = left_cols
        self.right_cols = right_cols
        self.full_len = len(self.left_cols)+len(self.right_cols)

    def __call__(self, row_left, row_right):
        res = [None] * self.full_len

        for col in self.left_cols:
            res[col[0]] = str(row_left[col[1]])
        
        for col in self.right_cols:
            res[col[0]] = str(row_right[col[1]])

        return res

