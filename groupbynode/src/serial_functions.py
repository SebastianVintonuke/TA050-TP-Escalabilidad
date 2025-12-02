import json


def json_dump_state(state):
	return json.dumps(state).encode()
def json_load_state(state):
	return json.loads(state.decode())


def json_dump_changes(changes):
	return json.dumps(changes).encode()
def json_load_changes(changes):
	return json.loads(changes.decode())

