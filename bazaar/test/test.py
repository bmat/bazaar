import io
import os
import shutil
import unittest

from pymongo import MongoClient

try:
    from bazaar.bazaar import BufferWrapper, FileSystem
except ImportError:
    import sys
    sys.path.insert(1, '.')
    from bazaar.bazaar import BufferWrapper, FileSystem


TEST_MONGO_URI = "mongodb://localhost/bazaar_test"


class TestFileSystem(unittest.TestCase):
    def setUp(self):

        tmp_dir = "/tmp/test"
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
        os.mkdir(tmp_dir)
        self.fs = FileSystem(tmp_dir, db_uri=TEST_MONGO_URI)
        self.fs.db.drop()

    def test_create_file(self):
        content = "This is a test"
        path = "/my_file.txt"
        namespace = "test"
        self.fs.put(path=path, content=content.encode(), namespace=namespace)
        saved_content = self.fs.get(path=path, namespace=namespace).decode()
        self.assertEqual(content, saved_content)

        # Update the file
        self.fs.put(path=path, content="lelelele".encode(), namespace=namespace)
        saved_content = self.fs.get(path=path, namespace=namespace).decode()
        self.assertEqual("lelelele", saved_content)

        # Same file with other namespace
        self.assertIsNone(self.fs.get(path=path, namespace="other"))

    def test_not_exist(self):
        # Same file with other namespace
        self.assertIsNone(self.fs.get(path="/notexists"))

    def test_delete_file(self):
        content = "This is a test"
        path = "/my_file.txt"
        namespace = "test"
        self.fs.put(path=path, content=content.encode(), namespace=namespace)
        self.assertIsNotNone(self.fs.get(path=path, namespace=namespace))
        self.fs.remove(path=path, namespace=namespace)
        self.assertIsNone(self.fs.get(path=path, namespace=namespace))

    def test_list_file(self):
        self.assertEqual([], self.fs.list("/"))
        self.fs.put(path="/first", content="a".encode())
        self.fs.put(path="/dir1/file", content="a".encode())
        self.fs.put(path="/dir1/otherfile", content="a".encode())
        self.assertListEqual(["first"], self.fs.list("/"))
        self.assertListEqual(["file", "otherfile"], self.fs.list("/dir1"))

        # A third level
        self.fs.put(path="/dir1/subdir/prettyfile", content="a".encode())
        self.assertListEqual(["prettyfile"], self.fs.list("/dir1/subdir"))

        # With weird names
        self.fs.put(path="/dir11/./prettyfile", content="a".encode())
        self.assertListEqual(["prettyfile"], self.fs.list("/dir11/."))

        self.fs.put(path="/dir22/$dir/prettyfile", content="a".encode())
        self.assertListEqual(["prettyfile"], self.fs.list("/dir22/$dir"))

    def test_list_file_multilevel(self):
        # multilevel
        self.fs.put(path="/fichero1", content=b"a")
        self.fs.put(path="/dir1/fichero1.1", content=b"a")
        self.fs.put(path="/dir1/fichero1.2", content=b"a")
        self.fs.put(path="/dir2/fichero2.1", content=b"a")
        self.fs.put(path="/dir1/subdir1/a", content=b"a")
        self.fs.put(path="/dir1/subdir1/b", content=b"a")
        self.fs.put(path="/dir1/subdir1/subidr2/c", content=b"a")
        self.fs.put(path="/dir1/subdir2/pepe/cp", content=b"a")
        self.fs.put(path="/this/is/test/file", content=b"a")

        self.assertListEqual(["fichero1"], self.fs.list("/"))
        self.assertListEqual(["fichero1.1", "fichero1.2"], self.fs.list("/dir1"))
        self.assertListEqual(["fichero2.1"], self.fs.list("/dir2"))

        self.assertListEqual(["a", "b"], self.fs.list("/dir1/subdir1"))

        self.assertListEqual(["c"], self.fs.list("/dir1/subdir1/subidr2"))
        self.assertListEqual(["cp"], self.fs.list("/dir1/subdir2/pepe"))
        self.assertListEqual(["file"], self.fs.list("/this/is/test"))

        
    def test_exists(self):
        namespace = "test"
        self.assertFalse(self.fs.exists("/my_file.txt", namespace=namespace))
        content = "This is a test"
        path = "/my_file.txt"
        self.fs.put(path=path, content=content.encode(), namespace=namespace)
        self.assertTrue(self.fs.exists("/my_file.txt", namespace=namespace))
        self.assertFalse(self.fs.exists("/my_file.txt"))

    # Mongomock aggregate does not work
    def test_directories(self):
        self.assertEqual([], self.fs.list("/"))
        self.fs.put(path="/first", content="a".encode())
        self.fs.put(path="/dir1/file", content="a".encode())
        self.fs.put(path="/dir1/secondfile", content="a".encode())
        self.fs.put(path="/dir1/subdir/prettyfile", content="a".encode())
        self.fs.put(path="/dir1/subdir2/prettyfile", content="a".encode())

        self.assertListEqual(["dir1"], self.fs.list_dirs("/"))
        self.assertSetEqual({"subdir", "subdir2"}, set(self.fs.list_dirs("/dir1")))

    def test_directories_multilevel(self):
        # multilevel
        self.fs.put(path="/fichero1", content=b"a")
        self.fs.put(path="/dir1/fichero1.1", content=b"a")
        self.fs.put(path="/dir1/fichero1.2", content=b"a")
        self.fs.put(path="/dir2/fichero2.1", content=b"a")
        self.fs.put(path="/dir1/subdir1/a", content=b"a")
        self.fs.put(path="/dir1/subdir1/b", content=b"a")
        self.fs.put(path="/dir1/subdir1/subidr2/c", content=b"a")
        self.fs.put(path="/dir1/subdir2/pepe/cp", content=b"a")
        self.fs.put(path="/this/is/test/file", content=b"a")

        # assertCountEqual not only counts, is like assertEqual but ignoring the order of the elements in the array
        self.assertCountEqual(["dir1", "dir2", "this"], self.fs.list_dirs("/"))
        self.assertCountEqual(["subdir1", "subdir2"], self.fs.list_dirs("/dir1"))
        self.assertCountEqual([], self.fs.list_dirs("/dir2"))
        self.assertCountEqual(["subidr2"], self.fs.list_dirs("/dir1/subdir1"))
        self.assertCountEqual([], self.fs.list_dirs("/dir1/subdir1/subidr2"))
        self.assertCountEqual(["pepe"], self.fs.list_dirs("/dir1/subdir2"))
        self.assertCountEqual(["test"], self.fs.list_dirs("/this/is"))

    def test_extras(self):
        self.fs.put(path="/first", content="a".encode())
        self.assertEqual({}, self.fs.get_extras(path="/first"))
        self.fs.set_extras(path="/first", extras={"foo": "bar"})
        self.assertEqual({"foo": "bar"}, self.fs.get_extras(path="/first"))


    def test_change_namespace(self):
        namespace = "test_1"
        namespace_2 = "test_2"

        # normal case
        self.fs.put(namespace=namespace, path="/original", content="a".encode())
        self.assertTrue(self.fs.exists(path="/original", namespace=namespace))
        self.assertFalse(self.fs.exists(path="/original", namespace=namespace_2))
        self.assertTrue(self.fs.change_namespace(path="/original", from_namespace=namespace, to_namespace=namespace_2))
        self.assertFalse(self.fs.exists(path="/original", namespace=namespace))
        self.assertTrue(self.fs.exists(path="/original", namespace=namespace_2))

        # wrong cases
        self.assertFalse(self.fs.change_namespace(path="/not_exists", from_namespace=namespace, to_namespace=namespace_2))
        self.fs.put(namespace=namespace, path="/original", content="a".encode())
        self.assertFalse(self.fs.change_namespace(path="/original", from_namespace=namespace, to_namespace=namespace_2))

    def assert_no_error(self, function, *args, **kwargs):
        raised = False
        try:
            function(*args, **kwargs)
        except:
            raised = True
        self.assertFalse(raised)

    def test_open_create(self):
        self.assert_no_error(self._open_create_file)

    def test_open_existing(self):
        self.assert_no_error(self._open_existing_file)

    def _open_create_file(self):
        example_file = self.fs.open('example.txt', 'w', namespace='example-namespace')
        example_file.close()

    def _open_existing_file(self):
        path, namespace = self._create_hello_world_file()
        example_file = self.fs.open(path, 'r', namespace=namespace)
        example_file.close()

    def test_open_context_handler(self):
        path, namespace = self._create_hello_world_file()
        with self.fs.open(path, 'r', namespace=namespace) as hello_world_file:
            self.assertEqual(hello_world_file.read(), 'Hello world!')

    def _create_hello_world_file(self):
        path = 'example.txt'
        namespace = 'example-namespace'
        self.fs.put(path, b'Hello world!', namespace=namespace)
        return path, namespace


class TestBufferWrapper(unittest.TestCase):
    test_file_dict = {'name': 'test_file', 'namespace': 'test-namespace'}

    def setUp(self):
        self.mongo_client = MongoClient(host=TEST_MONGO_URI)
        self.db = self.mongo_client.get_default_database().file
        self.db.drop()

    def create_test_file(self):
        test_file_data = self.test_file_dict.copy()
        test_file_data['size'] = 0
        self.db.insert_one(test_file_data)
        return test_file_data

    def wrapper_factory(self, wrapped_object=None, file_data=None, default_mode='r'):
        wrapped_object = wrapped_object or io.TextIOWrapper(io.BytesIO())
        try:
            wrapped_object.mode = getattr(wrapped_object, 'mode', None) or default_mode
        except AttributeError:  # BufferedWriter and BufferedReader mode can't be written
            pass
        file_data = file_data or {'name': 'test_file', 'namespace': 'test-namespace'}
        return BufferWrapper(wrapped_object=wrapped_object, file_data=file_data, db=self.db)

    def test_context_handler(self):
        wrapper = self.wrapper_factory()
        with wrapper as context_handled:
            self.assertEqual(wrapper, context_handled)

    def test_can_mode_change_size_read(self):
        wrapper = self.wrapper_factory(default_mode='r')
        self.assertFalse(wrapper.can_mode_change_size())

    def test_can_mode_change_size_write(self):
        for mode in {'w', 'w+', 'a', 'x'}:
            wrapper = self.wrapper_factory(default_mode=mode)
            self.assertTrue(wrapper.can_mode_change_size())

    def test_can_mode_change_size_buffered_reader(self):
        wrapper = self.wrapper_factory(wrapped_object=io.BufferedReader(io.BytesIO()))
        self.assertFalse(wrapper.can_mode_change_size())

    def test_can_mode_change_size_buffered_writer(self):
        wrapper = self.wrapper_factory(wrapped_object=io.BufferedWriter(io.BytesIO()))
        self.assertTrue(wrapper.can_mode_change_size())

    def test_update_file_size_non_existent(self):
        wrapper = self.wrapper_factory()
        with self.assertRaises(Exception) as e:
            wrapper.update_file_size(new_size=10)
        self.assertEqual(str(e.exception), 'Cannot update size of a non existent file')

    def test_update_file_size_existent(self):
        file_data = self.create_test_file()
        wrapper = self.wrapper_factory(file_data=file_data)
        self.assertEqual(wrapper.file_data['size'], 0)

        wrapper.update_file_size(10)
        updated_file_data = self.db.find_one(self.test_file_dict)
        self.assertEqual(updated_file_data['size'], 10)

    def test_update_file_size_if_needed_read_mode(self):
        file_data = self.create_test_file()
        wrapper = self.wrapper_factory(file_data=file_data)
        self.assertFalse(wrapper.update_file_size_if_needed())

    def test_update_file_size_if_needed_write_mode_no_size_change(self):
        file_data = self.create_test_file()
        wrapper = self.wrapper_factory(file_data=file_data, default_mode='w')
        self.assertFalse(wrapper.update_file_size_if_needed())

    def test_update_file_size_if_needed_write_mode_size_change(self):
        file_data = self.create_test_file()
        wrapper = self.wrapper_factory(file_data=file_data, default_mode='w')
        wrapper.wrapped_object.write('Hello world!')
        self.assertTrue(wrapper.update_file_size_if_needed())

    def test_close_updates_file_size(self):
        file_data = self.create_test_file()
        wrapper = self.wrapper_factory(file_data=file_data, default_mode='w')
        new_size = wrapper.wrapped_object.write('Hello world!')
        wrapper.close()
        updated_file_data = self.db.find_one(self.test_file_dict)
        self.assertEqual(updated_file_data['size'], new_size)

    def test_context_handler_updates_file_size(self):
        file_data = self.create_test_file()
        with self.wrapper_factory(file_data=file_data, default_mode='w') as wrapper:
            new_size = wrapper.wrapped_object.write('Hello world!')
        updated_file_data = self.db.find_one(self.test_file_dict)
        self.assertEqual(updated_file_data['size'], new_size)


if __name__ == '__main__':
    unittest.main()
