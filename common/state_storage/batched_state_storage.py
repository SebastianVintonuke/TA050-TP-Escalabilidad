import os
import json
import shutil
import time
from pathlib import Path
import logging

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

        self._ensure_dirs()

    def _ensure_dirs(self):
        for d in [
            self.metadata, self.states, self.packets,
            self.not_finished, self.not_applied,
            self.applied
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


    ## This loads the states and removes all except highest query_state
    def load_states(self):
        query_states = {} # result so that caller can do some extra logic If needed

        for query_id, packet_id, file in map(get_file_credentials, self.states.glob(f"*_*")):
            newest_state = query_states.setdefault(query_id, None)

            if newest_state == None:
                query_states[query_id] = [packet_id, file]
            elif newest_state[0] < packet_id: # Current state is newer

                # Delete prev newest since its an old version.. and for now there is handling for those.
                newest_state[1].unlink()
                # Replace newest state
                query_states[query_id] = [packet_id, file]
            else: # Current state is older so del
                file.unlink()

        for value in query_states.values():
            # Replace second vl with the deserial state
            value[1] =  self.manager.deserialize_state(value[1].read_bytes())

        return query_states
        
    # -------------------------------------------------------------
    #                   Defined design/contract
    # -------------------------------------------------------------

    def check_integrity(self, batch_size = 1):

        # 1. borrar not_finished (no confiables)
        clear_directory(self.not_finished)

        query_changes = {} # Internal cached data for changes to apply
        query_states = {} # result so that caller can do some extra logic If needed

        for query_id, packet_id, file in map(get_file_credentials, self.not_applied.glob(f"*_*")):
            items = query_changes.setdefault(query_id, [])
            items.append((packet_id, file))

        for query_id, changes in query_changes.items():
            changes.sort(key=lambda x: x[0]) #Inplace
            first_pck = changes[0][0]

            ## TODO Cleanup any previous state that could be lingering up to first_pck-batch_size-1//

            commit_ts, state = self._load_query_state(query_id,first_pck- batch_size) # Load prev state.
            if state == None:
                #If not state means packet_id -1 was not the current state ... was there concurrent changes? not supported for now
                print(f"Not supported concurrent changes at check integrity ? change packet: {first_pck} max batch: {batch_size} {self._state_file(query_id, first_pck- batch_size)} state did not exist! So discard")
                for _, file in changes: # Discard them
                    file.unlink()

                # Should load last state 
                # query_states[query_id] = ##                    
                continue

            i = 0
            new_exp_packet = first_pck#- batch_size
            while i < len(changes):

                # Not the best lol but more readable in some ways 
                packet_id, changes_file = changes[i]

                ## Lets be clear... IF changes was commited then its always supposed packet_id == new_exp_packet in non concurrent versions
                if packet_id != new_exp_packet or changes_file.stat().st_mtime > commit_ts:
                    break
                
                # Apply changes on file
                changes_to_apply, count_msgs = self.manager.deserialize_changes(changes_file.read_bytes())
                state = self.manager.apply_changes(state, changes_to_apply)

                # Create temp file with new state.. check for conflicts? future stuff!
                new_file = self.not_finished / f"{query_id}_{packet_id}"
                new_file.write_bytes(self.manager.serialize_state(state)) # Manager.serialize? or just str?

                # with open_file(new_file, "w") as f:
                #   f.write(str(state)) # Manager.serialize? or just str?

                ## Atomic replace/move of file 
                new_file.replace(self.states / f"{query_id}_{packet_id}")

                ## Del previous one! guaranteed to exist .. else would not be here.. else it should throw an error
                (self.states / f"{query_id}_{packet_id-1}").unlink()


                # No need to tag or do something to know wether to ack an already handled packet or not
                # packet id is sequential. So If a new packet has one that is less thats it, already acked.

                # delete file! not_applied, acks already marked. If failed right before then this changes file on next recovery would be discarded since 
                # first_pck-packet_size would not be the state.
                changes_file.unlink()

                new_exp_packet = packet_id+count_msgs
                i+=1


            while i < len(changes): #unlink any remaining change since its  modify time after commit...
                #Assumed higher packet id was handled after! i.e sequential handling .. send nack?
                # Extra overhead for logging... but this one happens just once.
                file = changes[i][1]
                if file.stat().st_mtime <= commit_ts:
                    print(f"Discarding not applied change, id:{packet_id}, {file}. Not contiguios packet id range expected: {new_exp_packet}")
                else:
                    print(f"Discarding not applied change, id:{packet_id}, {file}. Not commited change")

                file.unlink()
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

    def write_changes(self, query_id, batch_packet_id, changes):
        """
        Se escribe el archivo en not_finished y se mueve a not_applied.
        """
        nf = self.not_finished / f"{query_id}_{batch_packet_id}"
        nf.write_bytes(self.manager.serialize_changes(changes)) 
        
        # For now we assume that you call this write changes with the batch changes not only 1 packet.
        nf.replace(self.not_applied / f"{query_id}_{batch_packet_id}")  # operación atómica

    # Move it/replace it from draft to applied
    def finish_changes(self, query_id, batch_packet_id):
        nf = self.not_finished / f"{query_id}_{batch_packet_id}"
        nf.replace(self.not_applied / f"{query_id}_{batch_packet_id}")  # operación atómica


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
    # Requires the ack_tags to have the names of every packet that should be acked
    # -------------------------------------------------------------
    def push_changes(self, query_id, batch_packet_id, new_state, count_msgs): 
        change_file = self.not_applied / f"{query_id}_{batch_packet_id}"
        if not change_file.exists():
            raise InvalidStateError(f"Not supported concurrent changes.. saved changes '{change_file}' did not exist!")
        #changes = None
        # with open_file(change_file, "r") as f:
        #    changes = self.manager.deserialize_changes(f)

        # Estado anterior
        prev_state_file = self.states / f"{query_id}_{batch_packet_id - count_msgs}"

        # # Deserializar estado anterior
        # prev_state = None
        # with open_file(prev_state_file, "r") as f:
        #    prev_state = self.manager.deserialize_state(f)


        new_file = self.not_finished / f"{query_id}_{batch_packet_id}"
        new_file.write_bytes(self.manager.serialize_state(new_state)) # Manager.serialize? or just str?

        ## Atomic replace/move of file 
        new_file.replace(self.states / f"{query_id}_{batch_packet_id}")

        ## Del previous one! guaranteed to exist .. else would not be here.. else it should throw an error
        if prev_state_file.exists():
            prev_state_file.unlink()
        else:
            logging.warning(f"Warning.. at push changes {batch_packet_id} prev state file did not exist {prev_state_file}")
            #raise InvalidStateError("Not supported concurrent changes.. prev state did not exist!")


        # No need to tag or do something to know wether to ack an already handled packet or not
        # packet id is sequential. So If a new packet has one that is less thats it, already acked.

        # delete file! not_applied... should always exist since not concurrent
        change_file.unlink()

    # -------------------------------------------------------------
    # 5. unregister_query ...
    # -------------------------------------------------------------
    def unregister_query(self, query_id):
        pass