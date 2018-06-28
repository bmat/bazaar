import unittest
from bazaar.bazaar import File, FileSystem
import shutil
import os


class TestFileSystem(unittest.TestCase):
    def setUp(self):

        tmp_dir = "/tmp/test"
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
        os.mkdir(tmp_dir)
        self.fs = FileSystem(tmp_dir)
        File.drop_collection()

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

    def test_directories(self):
        self.assertEqual([], self.fs.list("/"))
        self.fs.put(path="/first", content="a".encode())
        self.fs.put(path="/dir1/file", content="a".encode())
        self.fs.put(path="/dir1/secondfile", content="a".encode())
        self.fs.put(path="/dir1/subdir/prettyfile", content="a".encode())
        self.fs.put(path="/dir1/subdir2/prettyfile", content="a".encode())

        self.assertListEqual(["dir1"], self.fs.list_dirs("/"))
        self.assertSetEqual({"subdir", "subdir2"}, set(self.fs.list_dirs("/dir1")))


if __name__ == '__main__':
    unittest.main()
