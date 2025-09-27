

MAP_MONTH = "map_month"
MAP_SEMESTER = "map_semester"

class MapMonthAction:
	def __init__(self, init_year, col_year, col_month, col_out):
		self.init_year = init_year
		self.col_year = col_year
		self.col_month = col_month
		self.col_out = col_out

	def map_in(row):
		months = (int(row[col_year])- self.init_year) * 12 + int(row[col_month])
		row[col_out] = months

class MapSemesterAction:
	def __init__(self, init_year, col_year, col_month, col_out):
		self.init_year = init_year
		self.col_year = col_year
		self.col_month = col_month
		self.col_out = col_out

	def map_in(row):
		months = (int(row[col_year])- self.init_year) * 12 + int(row[col_month])
		row[col_out] = months//3



# Somebody external... might add actions here? somethinglike row_mapping.GENERAL_MAP_ACTIONS["action"]: ClassName
GENERAL_MAP_ACTIONS = {
	MAP_MONTH: MapMonthAction,
	MAP_SEMESTER: MapSemesterAction,
}

MAP_FIELD_OP = 0
MAP_FIELD_PARAMS = 1

"""
mapper serial should be ["action", {"arg1":dd, "arg2":3}]  ... considered using dict as it is just once at init that is not as costly.
eg:
["map_month", {"init_year": 2024, "col_year":"year","col_month":"month","col_out":"month"}]
"""
def load_mapper(mapper_serial):
	creator = GENERAL_MAP_ACTIONS.get(
			mapper_serial[MAP_FIELD_OP], None
		)
	return creator(**mapper_serial[MAP_FIELD_PARAMS])

def load_all_mappers(mappers_serial):
	all_mappers = []
	for mapper_serial in mappers_serial:
		all_mappers.append(load_mapper(mapper_serial))
	return all_mappers


def map_all(mappers, row):
	for mapper in mappers:
		mapper.map_in(row) #in place.

	return row # Just for chaining

def map_dict_to_vec(fields, row):
	res=[]
	for col in fields:
		res.append(str(row[col]))
	return res

def create_mapper_function(config):
	mappers = load_all_mappers(config["actions"])
	cols = config["out_cols"]
	def map_func(row):
		return map_dict_to_vec(cols, map_all(mappers, row))

	return map_func