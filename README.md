[![Build Status](https://travis-ci.org/bmat/bazaar.svg?branch=master)](https://travis-ci.org/bmat/bazaar)
# Bazaar
In a bazaar you can find anything, like in Bazaar. Every file for every project, no matters what it is. Bazaar is an
agnostic storage system which can saves the binary data in your local hard disc, S3, FTP... and the metadata of the
files into a mongo database.

# Installing
```bash
pip install bazaar
```


# Using

```python
from bazaar import FileSystem

f = FileSystem()

# 'Put' function always receive bytes, so in the case of text you must encode it
f.put("/my_text.txt", "hello world".encode())
# And 'get' returns bytes too
f.get("/my_text.txt")
# b'hello world'
f.list("/")
# ['my_text.txt']
b = f.remove("/my_text.txt")

# You don't need to manage directories, just use it
d = f.put("/dir/subdir/other", "hello world".encode())
f.list("/dir/subdir")

# Directories are listed using a different method
f.list_dirs("/")
# ['dir']
f.list("/")
# ['my_text.txt']

f.close()
```

Initialization options
========================
There are several scenarios when you configure the database for Bazaar, tell us if yours is not covered!

Don't care about anything
-------------------
```python
f = FileSystem()
```
It will create a directory called 'bazaar' in your workspace for storage and a database called 'bazaar' at localhost.

Use default mongo connection
----------------------------
You handle your mongoengine connection by your own way so bazaar does not have to handle anything about that
(use default connection)
```python
f = FileSystem(db_uri=None)
```

Use a fully different connection
--------------------------------
If you plan to use bazaar in multiple applications, this is the ideal option. A server dedicated to bazaar
```python
f = FileSystem(storage_uri='s3://foo:bar@my-bucket', db_uri='mongo://user:pass@mongohost/database')
```
Yes, by this way you have a fully external storage system that can be used in any project! There is a new parameter
called 'storage_uri', in the following section we will talk about it

Storage backends
================
Bazaar support many storages since it uses the awesome library [PyFilesystem2](https://docs.pyfilesystem.org/en/latest/).
It supports local storage, ftp, memory, amazon s3... And you can build your own storage if you need it!

If you are planning to use Amazon S3 you will need you install fs-s3fs
```bash
pip install fs-s3fs
```
The value of the parameters 'storage_uri' is passed directly to PyFilesystem2 so check their documentation for more
information


# Testing
Just run
```bash
python bazaar/test/test.py
```