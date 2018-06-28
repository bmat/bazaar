from mongoengine import Document, DateTimeField, IntField, StringField, DoesNotExist, register_connection
from collections import namedtuple
from fs import open_fs
from datetime import datetime
import os
import six

FileAttrs = namedtuple('FileAttrs', ["created", "updated", "name", "size", "namespace"])


class File(Document):
    created = DateTimeField()
    updated = DateTimeField()
    name = StringField()
    size = IntField()
    namespace = StringField()

    meta = {'db_alias': 'bazaar'}


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
        if namespace is None:
            namespace = self.namespace

        try:
            d = File.objects.get(name=path, namespace=namespace)
            with self.fs.open(six.u(str(d.id)), "rb") as f:
                return f.read()
        except DoesNotExist:
            return None

    def put(self, path, content, namespace=None):
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

            with self.fs.open(six.u(str(d.id)), "wb") as f:
                f.write(content)
        else:
            raise Exception("Path must starts with a slash /")

    def list(self, path, namespace=None):
        if namespace is None:
            namespace = self.namespace

        name = {"$regex": '^{dir}/([^\/]*)$'.format(dir=path if path != "/" else "")}
        files = File.objects(namespace=namespace, name=name)
        return [file.name.split("/")[-1] for file in files]

    def list_dirs(self, path, namespace=None):
        if namespace is None:
            namespace = self.namespace

        name = {"$regex": '^{dir}/([^\/]*)/([^\/]*)$'.format(dir=path if path != "/" else "")}
        files = File.objects(namespace=namespace, name=name)
        return list(set([file.name.split("/")[-2] for file in files]))

    def attrs(self, path, namespace=None):
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

