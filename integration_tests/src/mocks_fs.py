import time
import fnmatch

class StatObject:
    def __init__(self):
        self.st_mtime = 0
        self.count_modified_time_stamp = 0
        self.count_written_times = 0

    def update_mtime(self):
        self.st_mtime = time.time()


class MockPath:
    def __init__(self, fs, path):
        self.fs = fs
        self.path = path
        self.content = b""
        self.name = self.fs.get_name(self)

    def __hash__(self):
        return hash(self.path)
    def __eq__(self, other):
        return self.path == other.path

    def clone_to(self, path):
        res = MockPath(self.fs, path)
        res.content = self.content
        return

    def __truediv__(self, new_segment):
        # Simulate directory traversal or path extension
        new_path = self.path + "/" + new_segment
        return MockPath(self.fs, new_path)

    def get_children(self):
        return self.fs.get_children(self)
    def glob(self, pattern):
        all_children = self.get_children()
        children = list(key.strip(self.path) for key in all_children.keys())

        children = fnmatch.filter(children, pattern)

        children = list(all_children[self.path+"/"+path] for path in children)
        return children

    def touch(self):
        if self.path not in self.fs.paths:
            self.fs.paths[self.path] = self
        else:
            self.fs.add_count_modified_time_stamp(self.path)
        
        self.fs.update_mod_time(self.path)

        # Add to parent directory if not already there
        parent = self.fs.get_parent(self.path)
        if parent:
            children = parent.get_children()
            if self.path not in children:
                children[self.path] = self

    def rmchildren(self):
        for child_path, child in self.get_children().items():
            child.rmchildren()
            self.fs.delete_path(child)
        self.fs.reset_children(self)

    def mkdir(self, parents=False, exist_ok=False):
        # Create directories
        if self.path not in self.fs.paths:
            self.fs.paths[self.path] = self
        # Add to parent directory if not already there
        parent = self.fs.get_parent(self.path)
        if parent:
            parent.get_children()[self.path] = self

    def exists(self):
        return self.path in self.fs.paths

    def unlink(self):
        # print(f"------>UNLINK {self.path}")
        # Simulate file deletion
        self.fs.delete_path(self)
        parent = self.fs.get_parent(self.path)
        if parent:
            children = parent.get_children()
            if self.path in children:
                del children[self.path]

    def write_bytes(self, data):

        # print(f"------>WROTE BYTES TO {self.path} '{data.decode()}")
        self.content = data
        self.fs.paths[self.path] = self  # Update the filesystem with new content
        self.fs.add_count_written(self.path)
        self.fs.update_mod_time(self.path)

    def write_text(self, data):
        self.write_bytes(data.encode())

    def read_bytes(self):
        return self.content

    def read_text(self):
        return self.content.decode()

    def __repr__(self):
        return self.path

    def replace(self, new_path):
        # print(f"------>MOVE {self.path} to {new_path} {self.content.decode()}")
        self.unlink()
        self.path = str(new_path)
        self.name = self.fs.get_name(self)
        self.touch()

        self.fs.paths[self.path] = self  # Update the filesystem with new content

    def stat(self):
        return self.fs.stats[self.path]


    # Entered/started?
    def __enter__(self):
        pass

    # With clause exit or so
    def __exit__(self, *args):
        print("EXIT GOT ARGS?", args)


class MockFilesystem:
    def __init__(self):
        self.paths = {}
        self.deleted_paths ={}
        self.stats = {}
        self.children = {}

    def reset_children(self, path_obj):
        self.children[path_obj.path]=  {}

    def get_children(self, path_obj):
        return self.children.setdefault(path_obj.path, {})

    def update_mod_time(self, path):
        obj = self.stats.setdefault(path, StatObject())
        obj.update_mtime()

    def add_count_modified_time_stamp(self, path):
        obj = self.stats.setdefault(path, StatObject())
        obj.count_modified_time_stamp+=1
        obj.update_mtime()

    def add_count_written(self, path):
        obj = self.stats.setdefault(path, StatObject())
        obj.count_written_times+=1
        obj.update_mtime()

    def delete_path(self, path_obj):
        if path_obj.path in self.paths:
            del self.paths[path_obj.path]
            stat = self.stats.setdefault(path_obj.path, StatObject())
            del self.stats[path_obj.path]

            if path_obj.path in self.children:
                del self.stats[path_obj.path]

            
            deleted_times = self.deleted_paths.setdefault(path_obj.path, [])
            deleted_times.append((path_obj, stat))


    def open_file(self, path, mode):
        if isinstance(path, MockPath):
            if path.path in self.paths:
                return path
        elif path in self.paths:
            return self.paths[path]

        raise FileNotFoundError(f"{path} not found.")


    def copy_file(self, src_path, dst_path):

        if isinstance(src_path, MockPath):
            src_path = src_path.path

        if src_path in self.paths:
            copy = self.paths[src_path].clone_to(dst_path)
            self.paths[dst_path] = copy
            return 

        raise FileNotFoundError(f"{src_path} not found.")


    def clear_directory(self, path):
        # Clean up all files in a directory (recursively)
        self.paths[path.path].rmchildren()

    def create_new_path(self, path):
        path_obj = MockPath(self, path)
        self.paths[path] = path_obj
        return path_obj

    def get_parent(self, path):
        # Simple method to return the parent directory of a path (based on '/')
        parts = path.strip('/').split('/')
        if len(parts) > 1:
            parent_path = '/'.join(parts[:-1])
            return self.paths.get(parent_path)
        return None

    def get_name(self, path):
        parts = path.path.strip('/').split('/')
        return parts[-1]

