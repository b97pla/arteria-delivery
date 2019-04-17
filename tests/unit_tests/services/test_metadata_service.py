
import shutil
import tempfile
import unittest

from delivery.services.metadata_service import MetadataService

from tests import test_utils


class TestMetadataService(unittest.TestCase):

    def setUp(self):
        self.rootdir = tempfile.mkdtemp()
        self.metadata_service = MetadataService()

    def tearDown(self):
        shutil.rmtree(self.rootdir)

    def test_extract_samplesheet_data(self):
        runfolder = test_utils.unorganised_runfolder(self.rootdir)
        samplesheet_file, samplesheet_data = test_utils.samplesheet_file_from_runfolder(runfolder)
        self.assertListEqual(samplesheet_data, self.metadata_service.extract_samplesheet_data(samplesheet_file))
