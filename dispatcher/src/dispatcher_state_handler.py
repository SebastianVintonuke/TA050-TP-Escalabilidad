
class DispatcherNodeStateManager:
    def __init__(self):
        pass

    def apply_changes(self, user_state, _changes):
        return user_state

    def deserialize_state(self, state):
        return state

    def serialize_changes(self, _user_state):
        return b""

    def deserialize_changes(self, _changes_bytes):
        return "", 1

    def serialize_state(self, _user_state):
        return b""

    def serialize_initial_state(self, _user_state):
        return b""