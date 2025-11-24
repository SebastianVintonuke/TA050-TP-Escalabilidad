import os
import json
import shutil
import time
import logging
from pathlib import Path

PathType =Path # In tests it would be replaced by a mock one
open_file = open # Also replaced on mocks
copy_file = shutil.copy # Also replaced on mocks

def clear_directory(directory: Path):
    # Deletes file by file and then the directoy... pc could shutdown in the middle
    # but it would not corrupt filesystem dentry just not delete all files or so...
    if directory.exists():
        shutil.rmtree(directory)
    directory.mkdir(parents=True, exist_ok=True)

def get_file_credentials(file):
    parts = file.name.split("_")
    # Query id is all but last part, then second value is version id
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
        self.cancelled_queries = self.metadata / "cancelled"

        self.states = self.base / "states"
        self.versions = self.base / "versions"

        # subcarpetas de versions
        self.not_finished = self.versions / "not_finished"
        self.not_applied = self.versions / "not_applied"
        self.applied = self.versions / "applied"

        self._ensure_dirs()

        self.cached_cancelled = set(self.cancelled_queries.glob("*")) # Load every cancelled query

    def _ensure_dirs(self):
        for d in [
            self.metadata, self.states, self.versions,
            self.not_finished, self.not_applied,
            self.applied, self.cancelled_queries, (self.base / "backups")
        ]:
            d.mkdir(parents=True, exist_ok=True)

    # -------------------------------------------------------------
    #                 Commit timestamp management
    # -------------------------------------------------------------
    def _commit_file(self, query_id):
        return self.metadata / f"{query_id}_commit"

    def _get_commit_timestamp(self, query_id):
        f = self._commit_file(query_id)
        if not f.exists():
            return 0
        return f.stat().st_mtime

    def _update_commit_timestamp(self, query_id):
        f = self._commit_file(query_id)
        f.touch() # Touch usa syscall utime o similes... que es atomica a nivel syscall.



    ## This loads the states and removes all except highest query_state
    def load_states(self):
        query_states = {} # result so that caller can do some extra logic If needed

        for query_id, version_id, file in map(get_file_credentials, self.states.glob(f"*_*")):
            newest_state = query_states.setdefault(query_id, None)

            if newest_state == None:
                query_states[query_id] = [version_id, file]
            elif newest_state[0] < version_id: # Current state is newer

                # Delete prev newest since its an old version.. and for now there is handling for those.
                newest_state[1].unlink()
                # Replace newest state
                query_states[query_id] = [version_id, file]
            else: # Current state is older so del
                file.unlink()

        for value in query_states.values():
            # Replace second vl with the deserial state
            
            value[1] =  self.manager.deserialize_state(value[1].read_bytes())

        return query_states

    # -------------------------------------------------------------
    #                   Defined design/contract
    # -------------------------------------------------------------

    def check_integrity(self):

        # 1. borrar not_finished (no confiables)
        clear_directory(self.not_finished)

        query_changes = {} # Internal cached data for changes to apply
        query_states = {} # result so that caller can do some extra logic If needed

        for query_id, version_id, file in map(get_file_credentials, self.not_applied.glob(f"*_*")):
            items = query_changes.setdefault(query_id, [])
            items.append((version_id, file))

        for query_id, changes in query_changes.items():
            changes.sort(key=lambda x: x[0]) #Inplace
            first_pck = changes[0][0]


            base_version = first_pck-1
            base_state_file = self.states / f"{query_id}_{base_version}"
            

            if not base_state_file.exists():
                print(f"Not supported concurrent changes at check integrity ? {base_state_file} state did not exist!")
                for _, file in changes: # Discard them
                    file.unlink()                
                continue

            commit_ts= self._get_commit_timestamp(query_id)
            state = self.manager.deserialize_state(base_state_file.read_bytes())

            i = 0

            while i < len(changes) and changes[i][1].stat().st_mtime <= commit_ts:

                # Apply changes on file/version
                version_id = changes[i][0]
                changes_to_apply, count_versions = self.manager.deserialize_changes(changes[i][1].read_bytes())

                if base_version < version_id- count_versions: # There is a gap, new version cannot be merged at least we assume so and abort/discard.
                    changes[i][1].unlink()
                    i+=1
                    continue

                state = self.manager.apply_changes(state, changes_to_apply)

                # Create temp file with new state.. check for conflicts? future stuff!
                new_file = self.not_finished / f"{query_id}_{version_id}"
                new_file.write_bytes(self.manager.serialize_state(state)) # Manager.serialize? or just str?

                ## Atomic replace/move of file 
                new_file.replace(self.states / f"{query_id}_{version_id}")

                ## Del previous one/ base version! guaranteed to exist .. else would not be here.. else it should throw an error
                (self.states / f"{query_id}_{base_version}").unlink()

                # delete file! not_applied
                changes[i][1].unlink()

                base_version = version_id #For next change/version this should be the base version.
                i+=1

            while i < len(changes): #unlink any remaining change since its  modify time after commit...
                changes[i][1].unlink()
                i+=1

            query_states[query_id] = state # Save on res
        return query_states




    # -------------------------------------------------------------
    # 1. register_query and management
    # -------------------------------------------------------------

    def register_query(self, query_id, metadata, initial_version_id = 0):
        """
        Check if query id state/ commit time and so on exists.
        """
        file = self._commit_file(query_id)
        if not file.exists(): # Only do touch if it does not exist.. else would modify timestamp
            # First add state file that also is missing If commit time one is.. so that 
            # If it crashes before creating commit file then at most you would create again or so these ones
            file_state = self.states / f"{query_id}_{initial_version_id}" # First/initial state
            file_state.touch()
            # Since commit file not created no issues with having it corrupted. If it crashes here.
            file_state.write_bytes(self.manager.serialize_initial_state(metadata))

            # Now create commit one
            file.touch()

    def is_query_registered(self, query_id):
        return self._commit_file(query_id).exists()

    # -------------------------------------------------------------
    # 5. unregister_query ... 
    # -------------------------------------------------------------
    def unregister_query(self, query_id):
        for file in self.states.glob(f"{query_id}_*"):
            file.unlink() # Delete all states saved

        # Since it could crash in the middle of register query we want to do the states deleting always just in case. Commit file might not exist
        file = self._commit_file(query_id)
        if file.exists():
            file.unlink()


    #
    # 6. Check Cancelled query ids, since we need to persist them to avoid saving unnecesary messages.
    ### Since not concurrent yet we can do a simple cache...
    def cancel_query(self, query_id):
        file = self.cancelled_queries / query_id
        self.cached_cancelled.add(file)        
        file.touch()

    def is_cancelled_query(self, query_id):        
        file = self.cancelled_queries / query_id
        return file in self.cached_cancelled
        # return  file.exists()

    ## For debugging purposes and so on
    def backup_query(self, query_id):
        newest_state = None
        for query_id, version_id, file in map(get_file_credentials, self.states.glob(f"{query_id}_*")):
            if newest_state == None or newest_state[0] < version_id:
                newest_state= [version_id, file]

        if newest_state == None:
            return

        copy_file(new_state[1], self.base/ f"backups/{new_state[1].name}")

    def backup_query_final(self, query_id, version_id, state):
        out_file = self.base/ f"backups/{query_id}_{version_id}_final"
        out_file.touch()

        out_file.write_bytes(self.manager.serialize_state(state))



    # -------------------------------------------------------------
    # 2. add_changes
    # -------------------------------------------------------------

    def write_changes(self, query_id, version_id, changes):
        """
        Se escribe el archivo en not_finished y se mueve a not_applied.
        """
        nf = self.not_finished / f"{query_id}_{version_id}"
        nf.write_bytes(self.manager.serialize_changes(changes)) 
        nf.replace(self.not_applied / f"{query_id}_{version_id}")  # operación atómica

    # -------------------------------------------------------------
    # 3. commit_changes
    # -------------------------------------------------------------

    def commit_changes(self, query_id):
        self._update_commit_timestamp(query_id)



    def calculate_new_state(self, base_state, changes):
        return self.manager.apply_changes(base_state, changes)

    # From base version in storage.
    def get_new_state(self, query_id, version_id, changes, count_versions = 1):
        base_state_file = self.states / f"{query_id}_{version_id - count_versions}"

        if not base_state_file.exists():
            raise InvalidStateError(f"Base version for new state {base_state_file} does not exist, storage allows only for sequential/exact version calculation.")

        # base_state = None
        # with open_file(base_state_file, "r") as f:

        base_state = self.manager.deserialize_state(base_state_file.read_bytes())

        return self.manager.apply_changes(base_state, changes)

    # -------------------------------------------------------------
    # 4. push_changes
    ## Lets assume caller has the changes saved on change_file... change file is just for backup in case of a crash.
    ## And also has prev state/ base version/state since we assume non concurrent modifying 
    ## SOO essentially received the new state calculated from get new state
    # -------------------------------------------------------------
    def push_changes(self, query_id, version_id, new_state, count_versions = 1): ## Lets 
        change_file = self.not_applied / f"{query_id}_{version_id}"
        if not change_file.exists():
            raise InvalidStateError(f"Not supported concurrent changes.. saved changes/version '{change_file}' did not exist!")

        new_file = self.not_finished / f"{query_id}_{version_id}"
        new_file.write_bytes(self.manager.serialize_state(new_state)) # Manager.serialize? or just str?

        ## Atomic replace/move of file 
        # print(f"Saving new version! '{query_id}_{version_id}' base ver is allegedly {version_id-count_versions}")
        new_file.replace(self.states / f"{query_id}_{version_id}")


        base_state_file = self.states / f"{query_id}_{version_id - count_versions}"

        ## Del previous/base state! For now allow it to not exist... checks for consistency should be at get_new_state or so.
        if base_state_file.exists():
            base_state_file.unlink()
        else:
            # raise InvalidStateError(f"Warning.. at push changes version: {version_id} base state file did not exist {base_state_file}")
            logging.warning(f"Warning.. at push changes version: {version_id} base state file did not exist {base_state_file}")


        # delete file! not_applied... should always exist since not concurrent
        change_file.unlink()

