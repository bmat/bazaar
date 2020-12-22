from pymongo import MongoClient
from collections import namedtuple
from fs import open_fs
from datetime import datetime
import os
import re

FileAttrs = namedtuple('FileAttrs', ["created", "updated", "name", "size", "namespace"])


# class File(Document):
#     created = DateTimeField()
#     updated = DateTimeField()
#     name = StringField()
#     size = IntField()
#     namespace = StringField()
#     extras = DictField()
#
#     meta = {'db_alias': 'bazaar'}


class BufferWrapper(object):
    def __init__(self, wrapped_object, file_data, db):
        self.wrapped_object = wrapped_object
        self.file_data = file_data
        self.db = db

    def __getattr__(self, attr):
        orig_attr = self.wrapped_object.__getattribute__(attr)
        if callable(orig_attr):
            def hooked(*args, **kwargs):
                if attr == "close":
                    file_size = self.wrapped_object.tell()
                    r = self.db.update_one({"name": self.file_data["name"], "namespace": self.file_data["namespace"]}, {"$set": {"size": file_size}})
                    # In case source does not exist, matched_count is 0
                    if r.matched_count > 0:
                        raise Exception("Cannot update size of a non existent file")
                result = orig_attr(*args, **kwargs)
                if result == self.wrapped_object:
                    return self
                return result

            return hooked
        else:
            return orig_attr


class FileSystem(object):

    def __init__(self, storage_uri=None, db_uri="mongodb://localhost/bazaar", namespace=""):
        if storage_uri is None:
            storage_uri = "bazaar"
            if not os.path.exists(storage_uri):
                os.mkdir(storage_uri)

        if db_uri is None:
            self.mongo = MongoClient()
        else:
            self.mongo = MongoClient(host=db_uri)

        self.fs = open_fs(storage_uri)
        self.db = self.mongo.get_default_database().file
        self.namespace = namespace

    def get(self, path, namespace=None):
        path = self.sanitize_path(path, False)
        if namespace is None:
            namespace = self.namespace

        d = self.db.find_one({"name": path, "namespace": namespace})
        if d is not None:
            with self.fs.open(str(d["_id"]), "rb") as f:
                return f.read()

    def open(self, path, mode, namespace=None):
        path = self.sanitize_path(path, False)
        if namespace is None:
            namespace = self.namespace

        d = self.db.find_one({"name": path, "namespace": namespace}, {"_id": 1})
        # Database information
        if d is not None:
            filename = str(d["_id"])
            new_file = False
        else:
            if "w" in mode:
                insert_info = self.db.insert_one({
                    "name": path,
                    "namespace": namespace,
                    "created": datetime.utcnow(),
                    "updated": datetime.utcnow()
                })
                filename = str(insert_info.inserted_id)
                new_file = True
            else:
                raise FileNotFoundError("[Errno 2] No such file or directory: '{filename}'".format(filename=path))

        # File bytes storing
        try:
            file = self.fs.open(filename, mode)
            file.close()  # Force the file creation to force error and avoid keeping the entity in the database but not the file
            file = self.fs.open(filename, mode)
            return BufferWrapper(file, d, self.db)
        except Exception as e:
            if new_file:
                # Only remove when creating the file, otherwise we could remove a valid entry (eg if database is ok and storage is not)
                self.db.delete_one({"name": path, "namespace": namespace})
                d.delete()
            raise e

    def change_namespace(self, path, from_namespace, to_namespace):
        path = self.sanitize_path(path, False)
        # Destination should not exists
        if self.db.find_one({"name": path, "namespace": to_namespace}, {"_id": 1}) is not None:
            return False

        # Perform the update
        r = self.db.update_one({"name": path, "namespace": from_namespace}, {"$set": {"namespace": to_namespace, "updated": datetime.utcnow()}})
        # In case source does not exist, matched_count is 0
        return r.matched_count > 0

    def set_extras(self, path, extras, namespace=None):
        path = self.sanitize_path(path, False)
        if namespace is None:
            namespace = self.namespace

        # Perform the update
        r = self.db.update_one({"name": path, "namespace": namespace}, {"$set": {"extras": extras, "updated": datetime.utcnow()}})
        return r.matched_count > 0

    def get_extras(self, path, namespace=None):
        path = self.sanitize_path(path, False)
        if namespace is None:
            namespace = self.namespace

        d = self.db.find_one({"name": path, "namespace": namespace}, {"extras": 1})
        if d is not None:
            return d.get("extras", {})
        else:
            return {}

    def put(self, path, content, namespace=None):
        path = self.sanitize_path(path, False)
        if namespace is None:
            namespace = self.namespace

        d = self.db.find_one({"name": path, "namespace": namespace}, {"size": 1, "updated": 1})
        new_file = d is None

        if new_file:
            insert_info = self.db.insert_one({
                "name": path,
                "namespace": namespace,
                "created": datetime.utcnow(),
                "updated": datetime.utcnow(),
                "size": len(content)
            })
            filename = str(insert_info.inserted_id)
        else:
            self.db.update_one({"name": path, "namespace": namespace}, {"$set": {"size": len(content), "updated": datetime.utcnow()}})
            filename = str(d["_id"])

        try:
            with self.fs.open(filename, "wb") as f:
                f.write(content)
        except Exception as e:
            if new_file:
                # Only remove when creating the file, otherwise we could remove a valid entry (eg if database is ok and storage is not)
                self.db.delete_one({"name": path, "namespace": namespace})
            else:
                # Backup data
                self.db.update_one({"name": path, "namespace": namespace}, {"$set": {"size": d["size"], "updated": d["updated"]}})
            raise e

    def list(self, path, namespace=None):
        path = self.sanitize_path(path, True)
        if namespace is None:
            namespace = self.namespace

        # We search for paths that starts with the provided path (directory), the something without a slash (filename)
        # and the end, because if we have another slash this is a directory
        files = self.db.find({"namespace": namespace, "name": {'$regex': f'^{re.escape(path)}[^/]+$'}})
        # To make it faster, a raw mongo query
        return [file["name"].rsplit("/", 1)[-1] for file in files]

    @staticmethod
    def sanitize_path(path, directory):
        path = os.path.realpath(path)

        if directory:
            if not path.endswith("/"):
                path += "/"

        if not path.startswith("/"):
            path = "/" + path

        return path

    def list_dirs(self, path, namespace=None):
        path = self.sanitize_path(path, True)
        # query = list_dir_query(path)
        if namespace is None:
            namespace = self.namespace

        pipeline = [
            # 1. Get things that are: path/{dir}/{something} just to be sure that {dir} is a directory and not a file
            {"$match": {'namespace': namespace, 'name': {'$regex': f'^{re.escape(path)}([^/]+/)'}}},
            # 2. Remove the provided path from the path. path/dir1/file -> dir1/file
            {"$project": {'name': {"$substr": ["$name", len(path), {"$strLenCP": "$name"}]}}},
            # 3. Split a get the first element. dir1/file -> dir. It also works for dir1/subdir/file -> dir1
            {"$project": {'name': {"$arrayElemAt": [{"$split": ["$name", "/"]}, 0]}}},
            # 4. Group to avoid duplicates (a subdirectory with several files will cause these duplicates)
            {"$group": {"_id": "$name"}}
        ]
        return [f["_id"] for f in self.db.aggregate(pipeline)]

    def rename(self, old_path, new_path, namespace=None):
        old_path = self.sanitize_path(old_path, False)
        new_path = self.sanitize_path(new_path, False)
        if namespace is None:
            namespace = self.namespace

        r = self.db.update_one({"name": old_path, "namespace": namespace}, {"$set": {"name": new_path}})
        return r.matched_count > 0

    def attrs(self, path, namespace=None):
        path = self.sanitize_path(path, False)
        if namespace is None:
            namespace = self.namespace

        f = self.db.find_one({"name": path, "namespace": namespace})
        if f is not None:
            return FileAttrs(
                created=f["created"],
                updated=f["updated"],
                name=f["name"].rsplit("/")[-1],
                size=f["size"],
                namespace=f["namespace"]
            )

    def remove(self, path, namespace=None):
        path = os.path.realpath(path)
        if namespace is None:
            namespace = self.namespace

        r = self.db.delete_one({"name": path, "namespace": namespace})
        return r.deleted_count > 0

    def close(self):
        self.fs.close()

    def exists(self, path, namespace=None):
        path = self.sanitize_path(path, False)
        if namespace is None:
            namespace = self.namespace

        return self.db.find_one({"name": path, "namespace": namespace}, {"_id": 1}) is not None
