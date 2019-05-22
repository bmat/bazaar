from mongoengine import Document, DateTimeField, IntField, StringField, DoesNotExist, register_connection, DictField
from collections import namedtuple
from fs import open_fs
from datetime import datetime
import os
import six
import re

FileAttrs = namedtuple('FileAttrs', ["created", "updated", "name", "size", "namespace"])


class File(Document):
    created = DateTimeField()
    updated = DateTimeField()
    name = StringField()
    size = IntField()
    namespace = StringField()
    extras = DictField()

    meta = {'db_alias': 'bazaar'}


class BufferWrapper(object):
    def __init__(self, wrapped_object, file_document):
        self.wrapped_object = wrapped_object
        self.file_document = file_document

    def __getattr__(self, attr):
        orig_attr = self.wrapped_object.__getattribute__(attr)
        if callable(orig_attr):
            def hooked(*args, **kwargs):
                if attr == "close":
                    self.file_document.size = self.wrapped_object.tell()
                    self.file_document.save()
                result = orig_attr(*args, **kwargs)
                if result == self.wrapped_object:
                    return self
                return result

            return hooked
        else:
            return orig_attr

# Mongo queries for list directories

root = [
{
    "$match":
    {
        "name": {'$regex': '^([^\\/]*)/'}
    }
},
{
    "$project" : {
        "name": {
            "$split": ["$name", "/"]
            }
    }
},
{
    "$match": {
        'name.2': {"$exists": True}
    }
},
{
    "$project" : {
        "name": {
            "$arrayElemAt": ["$name", 1]
            }
    }
},
{
    "$group": {
        "_id": "$name"
    }
}
]

def list_dir_query(path):
    if path == "/":
        return root
    else:
        return [
        {
            "$match":
            {
                "name": {'$regex': '^{path}\\/'.format(path=re.escape(path))}
            }
        },
        {
            "$project" : {
                "name": {
                    "$split": ["$name", path]
                    }
            }
        }
        ,
        {
            "$project" : {
                "name": {
                    "$split": [{"$arrayElemAt": ["$name", 1]}, "/"]
                    }
            }
        },
        {
        "$match": {
            'name.2': {"$exists": True}
        }
        }
        ,
        {
            "$project" : {
                "name": {
                    "$arrayElemAt": ["$name", 1]
                    }
            }
        },
        {
            "$group": {
                "_id": "$name"
            }
        }

        ]


class FileSystem(object):

    def __init__(self, storage_uri=None, db_uri="mongodb://localhost/bazaar", namespace=""):
        if storage_uri is None:
            storage_uri = "bazaar"
            if not os.path.exists(storage_uri):
                os.mkdir(storage_uri)

        if db_uri is None:
            File._meta['db_alias'] = "default"
        else:
            register_connection("bazaar", host=db_uri)

        self.fs = open_fs(storage_uri)

        self.namespace = namespace

    def get(self, path, namespace=None):
        path = os.path.realpath(path)
        if namespace is None:
            namespace = self.namespace

        try:
            d = File.objects.get(name=path, namespace=namespace)
            with self.fs.open(six.u(str(d.id)), "rb") as f:
                return f.read()
        except DoesNotExist:
            return None

    def open(self, path, mode, namespace=None):
        if path.startswith("/"):
            path = os.path.realpath(path)
            if namespace is None:
                namespace = self.namespace

            try:
                d = File.objects.get(name=path, namespace=namespace)
            except DoesNotExist:
                if "w" in mode:
                    d = File(name=path, namespace=namespace)
                    d.created = datetime.now()
                    d.updated = datetime.now()
                    d.save()
                else:
                    raise FileNotFoundError("[Errno 2] No such file or directory: '{filename}'".format(filename=path))
            try:
                file = self.fs.open(six.u(str(d.id)), mode)
                file.close()  # Force the file creation for force error and avoid to have the entity in the database but not the file
                file = self.fs.open(six.u(str(d.id)), mode)
                return BufferWrapper(file, d)
            except Exception as e:
                d.delete()
                raise e
        else:
            raise Exception("Path must starts with a slash /")

    def change_namespace(self, path, from_namespace, to_namespace):
        path = os.path.realpath(path)
        try:
            # Destination should not exists
            d = File.objects.get(name=path, namespace=to_namespace)
            return False
        except DoesNotExist:
            try:
                # But source should exists
                d = File.objects.get(name=path, namespace=from_namespace)
                d.namespace = to_namespace
                d.save()
            except DoesNotExist:
                return False

    def set_extras(self, path, extras, namespace=None):
        path = os.path.realpath(path)
        if namespace is None:
            namespace = self.namespace
        try:
            d = File.objects.get(name=path, namespace=namespace)
            d.extras = extras
            d.save()
        except DoesNotExist:
            return False

    def get_extras(self, path, extras, namespace=None):
        path = os.path.realpath(path)
        if namespace is None:
            namespace = self.namespace
        try:
            d = File.objects.get(name=path, namespace=namespace)
            return d.extras
        except DoesNotExist:
            return None

    def put(self, path, content, namespace=None):
        path = os.path.realpath(path)
        if namespace is None:
            namespace = self.namespace

        if path.startswith("/"):
            try:
                d = File.objects.get(name=path, namespace=namespace)
            except DoesNotExist:
                d = File(name=path, namespace=namespace)
                d.created = datetime.now()
            d.updated = datetime.now()
            d.size = len(content)
            d.save()
            try:
                with self.fs.open(six.u(str(d.id)), "wb") as f:
                    f.write(content)
            except Exception as e:
                d.delete()
                raise e
        else:
            raise Exception("Path must starts with a slash /")

    def list(self, path, namespace=None):
        path = os.path.realpath(path)
        if namespace is None:
            namespace = self.namespace

        name = {"$regex": '^{dir}\\/(?!.*(\\/))'.format(dir=re.escape(path) if path != "/" else "")}
        files = File.objects(namespace=namespace, name=name)
        return [file.name.split("/")[-1] for file in files]

    def list_dirs(self, path, namespace=None):
        path = os.path.realpath(path)
        query = list_dir_query(path)
        if namespace is None:
            namespace = self.namespace

        query[0]["$match"]["namespace"] = namespace
        return [f["_id"] for f in File.objects.aggregate(*query)]

    def rename(self, old_path, new_path, namespace=None):
        old_path = os.path.realpath(old_path)
        new_path = os.path.realpath(new_path)
        if namespace is None:
            namespace = self.namespace

        try:
            d = File.objects.get(name=old_path, namespace=namespace)
            d.name = new_path
            d.save()
            return True
        except DoesNotExist:
            return False

    def attrs(self, path, namespace=None):
        path = os.path.realpath(path)
        if namespace is None:
            namespace = self.namespace

        try:
            f = File.objects.get(namespace=namespace, name=path)
            return FileAttrs(
                created=f.created,
                updated=f.updated,
                name=f.updated,
                size=f.size,
                namespace=f.namespace
            )
        except DoesNotExist:
            return None

    def remove(self, path, namespace=None):
        path = os.path.realpath(path)
        if namespace is None:
            namespace = self.namespace
        try:
            d = File.objects.get(name=path, namespace=namespace)
            self.fs.remove(six.u(str(d.id)))
            d.delete()
            return True
        except DoesNotExist:
            return False

    def close(self):
        self.fs.close()

    def exists(self, path, namespace=None):
        path = os.path.realpath(path)
        if namespace is None:
            namespace = self.namespace

        try:
            File.objects.get(name=path, namespace=namespace)
            return True
        except DoesNotExist:
            return False


