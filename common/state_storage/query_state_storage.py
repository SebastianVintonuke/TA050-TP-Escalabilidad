import os
import json
import shutil
import time
from pathlib import Path

PathType =Path # In tests it would be replaced by a mock one
open_file = open # Also replaced on mocks

def clear_directory(directory: Path):
    # Deletes file by file and then the directoy... pc could shutdown in the middle
    # but it would not corrupt filesystem dentry just not delete all files or so...
    if directory.exists():
        shutil.rmtree(directory)
    directory.mkdir(parents=True, exist_ok=True)

def get_file_credentials(file):
    parts = file.name.split("_")
    # Query id is all but last part, then second value is packet id
    return ("_".join(parts[:-1]), int(parts[-1]), file)



IND_COMMIT_TS = 1
IND_QUERY_DATA = 0

class InvalidStateError(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class QueryStateStorage:
    def __init__(self, base_path, state_manager):
        # self.base = Path(base_path)
        self.base = PathType(base_path) 
        self.manager = state_manager
        
        # directorios fijos
        self.metadata = self.base / "metadata"
        self.states = self.base / "states"
        self.packets = self.base / "packets"

        # subcarpetas de packets
        self.not_finished = self.packets / "not_finished"
        self.not_applied = self.packets / "not_applied"
        self.applied = self.packets / "applied"
        self.finished = self.packets / "finished"

        self._ensure_dirs()

    def _ensure_dirs(self):
        for d in [
            self.metadata, self.states, self.packets,
            self.not_finished, self.not_applied,
            self.applied, self.finished
        ]:
            d.mkdir(parents=True, exist_ok=True)

    # -------------------------------------------------------------
    #                 Filenames parsers
    # -------------------------------------------------------------

    def _metadata_file(self, query_id):
        return self.metadata / query_id

    def _commit_file(self, query_id):
        return self.metadata / f"{query_id}_commit"

    def _packet_file(self, folder, query_id, packet_id):
        return folder / f"{query_id}_{packet_id}"

    def _finished_packet(self, folder, query_id, packet_id):
        return self.finished / f"{query_id}_{packet_id}"

    def _state_file(self, query_id, packet_id):
        return self.states / f"{query_id}_{packet_id}"

    # -------------------------------------------------------------
    #                 Commit timestamp management
    # -------------------------------------------------------------
    def _get_commit_timestamp(self, query_id):
        f = self._commit_file(query_id)
        if not f.exists():
            return 0
        return f.stat().st_mtime

    def _update_commit_timestamp(self, query_id):
        f = self._commit_file(query_id)
        f.touch() # Touch usa syscall utime o similes... que es atomica a nivel syscall.


    def _load_query_state(self, query_id, packet_id):
        state_file = self.states / f"{query_id}_{packet_id}"
        if state_file.exists():
            return (
                self._get_commit_timestamp(query_id),
                self.manager.deserialize_state(state_file.read_bytes())
                )

        ## IF packet_id state does not exist return (None, None)
        return (None, None)


    # -------------------------------------------------------------
    #                   Defined design/contract
    # -------------------------------------------------------------

    def check_integrity(self):

        # 1. borrar not_finished (no confiables)
        clear_directory(self.not_finished)

        query_changes = {} # Internal cached data for changes to apply
        query_states = {} # result so that caller can do some extra logic If needed

        for query_id, packet_id, file in map(get_file_credentials, self.not_applied.glob(f"*_*")):
            items = query_changes.setdefault(query_id, [])
            items.append((packet_id, file))

        # print("-----------> CHECK WITH")
        # print(query_changes)
        # print("----------->")
        for query_id, changes in query_changes.items():
            changes.sort(key=lambda x: x[0]) #Inplace
            first_pck = changes[0][0]

            ## TODO Cleanup any previous state that could be lingering up to first_pck-2//

            commit_ts, state = self._load_query_state(query_id,first_pck-1) # Load prev state.
            if state == None:
                #If not state means packet_id -1 was not the current state ... was there concurrent changes? not supported for now
                print(f"Not supported concurrent changes at check integrity ? {self._state_file(query_id, first_pck-1)} state did not exist!")
                for _, file in changes: # Discard them
                    file.unlink()
                continue

            i = 0
            while i < len(changes) and changes[i][1].stat().st_mtime <= commit_ts:
                # Apply changes on file
                changes_to_apply, ack_tags = self.manager.deserialize_changes(changes[i][1].read_bytes())
                state = self.manager.apply_changes(state, changes_to_apply)

                # Create temp file with new state.. check for conflicts? future stuff!
                packet_id = changes[i][0]
                new_file = self.not_finished / f"{query_id}_{packet_id}"
                new_file.write_bytes(self.manager.serialize_state(state)) # Manager.serialize? or just str?

                # with open_file(new_file, "w") as f:
                #   f.write(str(state)) # Manager.serialize? or just str?

                ## Atomic replace/move of file 
                print("SHOULD REPLACE? by ", self.states / f"{query_id}_{packet_id}")
                new_file.replace(self.states / f"{query_id}_{packet_id}")

                ## Del previous one! guaranteed to exist .. else would not be here.. else it should throw an error
                (self.states / f"{query_id}_{packet_id-1}").unlink()

                ## Create ack
                ack_file = self.finished / f"{ack_tags[0]}" # Just one ack tag assumed
                ack_file.touch()

                # delete file! not_applied
                changes[i][1].unlink()

                i+=1

            while i < len(changes): #unlink any remaining change since its  modify time after commit...
                #Assumed higher packet id was handled after! i.e sequential handling .. send nack?
                changes[i][1].unlink()
                i+=1

            query_states[query_id] = state # Save on res
        return query_states




    # -------------------------------------------------------------
    # 1. register_query
    # -------------------------------------------------------------

    def register_query(self, query_id, metadata, initial_packet_id = 0):
        """
        Check if query id state/ commit time and so on exists.
        """
        file = self._commit_file(query_id)
        if not file.exists(): # Only do touch if it does not exist.. else would modify timestamp
            # First add state file that also is missing If commit time one is.. so that 
            # If it crashes before creating commit file then at most you would create again or so these ones
            file_state = self.states / f"{query_id}_{initial_packet_id}" # First/initial state
            file_state.touch()
            # Since commit file not created no issues with having it corrupted. If it crashes here.
            file_state.write_bytes(self.manager.serialize_initial_state(metadata))

            # Now create commit one
            file.touch()


    # -------------------------------------------------------------
    # 2. add_changes
    # -------------------------------------------------------------

    def write_changes(self, query_id, packet_id, changes):
        """
        Se escribe el archivo en not_finished y se mueve a not_applied.
        """
        nf = self._packet_file(self.not_finished, query_id, packet_id)
        nf.write_bytes(self.manager.serialize_changes(changes)) 
        nf.replace(self.not_applied / f"{query_id}_{packet_id}")  # operación atómica

    # -------------------------------------------------------------
    # 3. commit_changes
    # -------------------------------------------------------------

    def commit_changes(self, query_id):
        self._update_commit_timestamp(query_id)


    def get_new_state(self, prev_state, changes):
        return self.manager.apply_changes(prev_state, changes)

    # -------------------------------------------------------------
    # 4. push_changes
    ## Lets assume caller has the changes saved on change_file... change file is just for backup in case of a crash.
    ## And also has prev state since we assume non concurrent modifying 
    ## SOO essentially received the new state calculated from get new state
    # -------------------------------------------------------------
    def push_changes(self, query_id, packet_id, new_state, ack_tags): ## Lets 
        change_file = self.not_applied / f"{query_id}_{packet_id}"
        if not change_file.exists():
            raise InvalidStateError(f"Not supported concurrent changes.. saved changes '{change_file}' did not exist!")
        #changes = None
        # with open_file(change_file, "r") as f:
        #    changes = self.manager.deserialize_changes(f)

        # Estado anterior
        prev_state_file = self.states / f"{query_id}_{packet_id - 1}"

        if not prev_state_file.exists():
            raise InvalidStateError("Not supported concurrent changes.. prev state did not exist!")
        # # Deserializar estado anterior
        # prev_state = None
        # with open_file(prev_state_file, "r") as f:
        #    prev_state = self.manager.deserialize_state(f)


        new_file = self.not_finished / f"{query_id}_{packet_id}"
        new_file.write_bytes(self.manager.serialize_state(new_state)) # Manager.serialize? or just str?

        ## Atomic replace/move of file 
        new_file.replace(self.states / f"{query_id}_{packet_id}")

        ## Del previous one! guaranteed to exist .. else would not be here.. else it should throw an error
        prev_state_file.unlink()

        ## Create ack
        ack_file = self.finished / f"{ack_tags[0]}" ## Assumed only 1 ack tag
        ack_file.touch()

        # delete file! not_applied... should always exist since not concurrent
        change_file.unlink()

    # -------------------------------------------------------------
    # 5. unregister_packet
    # -------------------------------------------------------------
    def ack_finished(self, ack_func):
        # for query_id, packet_id, file in map(get_file_credentials, self.finished.glob(f"*_*")):
        for file in self.finished.glob(f"*"):
            ## here file name is the ack tag...
            ack_func(file.name)
            file.unlink()

    def unregister_packet(self, tag):
        f = (self.finished / f"{tag}")
        if f.exists():
            f.unlink()
