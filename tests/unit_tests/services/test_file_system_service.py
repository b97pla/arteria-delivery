
import os
import shutil
import tempfile
import unittest

from delivery.services.file_system_service import FileSystemService


class TestFileSystemService(unittest.TestCase):

    @staticmethod
    def _tempfiles(dir, n):
        return [tempfile.mkstemp(dir=dir)[1] for i in range(n)]

    @staticmethod
    def _tempdirs(dir, n):
        return [tempfile.mkdtemp(dir=dir) for i in range(n)]

    def setUp(self):
        self.rootdir = tempfile.mkdtemp()
        self.dirs = []
        self.files = []
        self.dirs.extend(self._tempdirs(self.rootdir, 2))
        self.dirs.extend(self._tempdirs(self.dirs[1], 2))
        self.files.extend(self._tempfiles(self.rootdir, 3))
        self.files.extend(self._tempfiles(self.dirs[0], 3))
        self.files.extend(self._tempfiles(self.dirs[-1], 3))

    def tearDown(self):
        shutil.rmtree(self.rootdir)

    def test_list_files_recursively(self):
        self.assertListEqual(
            sorted(self.files),
            sorted(list(FileSystemService().list_files_recursively(self.rootdir)))
        )
