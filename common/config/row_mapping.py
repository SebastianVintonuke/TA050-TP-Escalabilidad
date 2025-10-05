## General row mappers


class NoActionRowMapper:
    # Project out should remove unnecesary columns and return a vector
    # of strings with the values for each field in order.
    def project_out(self, row):
        return row

    # Call is a shorthand for map and project
    def __call__(self, row):
        return row

    # map the row, assumed to be already passed through map_input so that its in valid format
    # for the mapper
    def map(self, row):
        return row

    # map input row
    # for now row will be converted from vec of values ["21","32","user"] to the
    # used format on mapper, in theory it could also be other formats since there is no type check
    # what types is allowed depends on implementation
    def map_input(self, row):
        return row


class RowProjectMapper(NoActionRowMapper):
    def __init__(self, out_cols):
        self.cols = out_cols

    # Row has to be indexable, could be either dict or vec of values
    def project_out(self, row):
        res = []
        for col in self.cols:
            res.append(str(row[col]))
        return res

    # Add only column projection logic, still no row type specific logic.
    def __call__(self, row):
        return self.project_out(row)


class DictConvertWrapperMapper:
    def __init__(self, in_cols, inner_mapper, out_cols):
        self.in_cols = in_cols
        self.inner_mapper = inner_mapper
        self.out_cols = out_cols

    # Map first from vector to dictionary, assumes row of type vector
    def map_input(self, row):
        res = {}
        for ind, col in enumerate(self.in_cols):
            res[col] = row[ind]
        return res

    # In project out, map from dict to vec
    def project_out(self, row):
        res = []
        for col in self.out_cols:
            res.append(str(row[col]))
        return res

    # Delegate to inner mapper, that is assumed to receive a dict.
    def map(self, row):
        return self.inner_mapper.map(row)

    def __call__(self, row):
        return self.project_out(self.map(row))


## Configurable row mapper
MAP_MONTH = "map_month"
MAP_SEMESTER = "map_semester"


class MapMonthAction:
    def __init__(self, init_year, col_year, col_month, col_out):
        self.init_year = init_year
        self.col_year = col_year
        self.col_month = col_month
        self.col_out = col_out

    def map_in(self, row):
        months = (int(row[self.col_year]) - self.init_year) * 12 + int(
            row[self.col_month]
        )
        row[self.col_out] = months


class MapSemesterAction:
    def __init__(self, init_year, col_year, col_month, col_out):
        self.init_year = init_year
        self.col_year = col_year
        self.col_month = col_month
        self.col_out = col_out

    def map_in(self, row):
        months = (int(row[self.col_year]) - self.init_year) * 12 + int(
            row[self.col_month]
        )
        row[self.col_out] = months // 6


# Somebody external... might add actions here? somethinglike row_mapping.GENERAL_MAP_ACTIONS["action"]: ClassName
GENERAL_MAP_ACTIONS = {
    MAP_MONTH: MapMonthAction,
    MAP_SEMESTER: MapSemesterAction,
}

ROW_CONFIG_OUT_COLS = "out_cols"
ROW_CONFIG_ACTIONS = "actions"

MAP_FIELD_OP = 0
MAP_FIELD_PARAMS = 1

"""
mapper serial should be ["action", {"arg1":dd, "arg2":3}]  ... considered using dict as it is just once at init that is not as costly.
eg:
["map_month", {"init_year": 2024, "col_year":"year","col_month":"month","col_out":"month"}]
"""


def load_mapper(mapper_serial):
    creator = GENERAL_MAP_ACTIONS.get(mapper_serial[MAP_FIELD_OP], None)
    return creator(**mapper_serial[MAP_FIELD_PARAMS])


def load_all_mappers(mappers_serial):
    all_mappers = []
    for mapper_serial in mappers_serial:
        all_mappers.append(load_mapper(mapper_serial))
    return all_mappers


# Configrable Row mapper is independent of row type
# It just has to be indexable, could be dict or vec If config col types are correct for row index type
class RowMapper(RowProjectMapper):
    def __init__(self, config):
        super().__init__(config[ROW_CONFIG_OUT_COLS])
        self.mappers = load_all_mappers(config.get(ROW_CONFIG_ACTIONS, []))

    def map(self, row):
        for mapper in self.mappers:
            mapper.map_in(row)  # in place.
        return row  # Just for chaining

    def __call__(self, row):
        return self.project_out(self.map(row))
