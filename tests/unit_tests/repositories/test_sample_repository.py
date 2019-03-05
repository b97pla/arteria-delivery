
import os
import unittest

from delivery.exceptions import FileNameParsingException
from delivery.repositories.sample_repository import RunfolderProjectBasedSampleRepository

from tests import test_utils


class TestSampleRepository(unittest.TestCase):

    def setUp(self):
        self.project = test_utils.FAKE_RUNFOLDERS[0].projects[0]
        no_samples = 3
        sample_names = ["Sample{}".format(str(i)) for i in range(no_samples)]
        sample_index = ["S{}".format(str(i)) for i in range(no_samples)]
        lane_no = [1, 2, 3]
        is_index = [False, True]
        read_no = [1, 2]
        sample_paths = [
            os.path.join(self.project.path, "Sample_{}".format(sample_names[0])),
            self.project.path,
            os.path.join(self.project.path, sample_names[2])
        ]
        self.fastq_files = []
        for i in range(no_samples):
            for ii in is_index:
                for r in read_no:
                    file_name = "_".join([
                        sample_names[i],
                        sample_index[i],
                        "L00{}".format(str(lane_no[i])),
                        "{}{}".format(
                            "I" if ii else "R",
                            str(r)
                        ),
                        "001.fastq.gz"])
                    self.fastq_files.append(os.path.join(sample_paths[i], file_name))
        file_system_service = test_utils.mock_file_system_service([], [], fastq_files=self.fastq_files)
        self.sample_repo = RunfolderProjectBasedSampleRepository(file_system_service=file_system_service)

    def test_get_samples(self):
        for sample in self.sample_repo.get_samples(self.project):
            self.assertEqual(self.project.name, sample.project_name)
            self.assertEqual(4, len(sample.sample_files))

    def test_sample_file_from_sample_path(self):
        bad_filenames = [
            "this-is-not-a-proper-fastq-file-name",
            "not_ok_S1_L002_R1_001.fastq",
            "not_ok_ACGGTG_L001_I1_001.fastq.gz",
            "will_not_work_S1_L010_R1_001.fastq.gz"]
        for bad_filename in bad_filenames:
            self.assertRaises(
                FileNameParsingException,
                self.sample_repo.sample_file_from_sample_path,
                bad_filename)

        sample_names = ["this-is-ok", "ok", "this_is_ok", "this_is_ok_S8_L008_R3"]
        sample_index = ["S0", "S1", "S999"]
        lane_no = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        is_index = [True, False]
        read_no = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        suffix = ["001.fastq.gz", "002.fastq.gz", "999.fastq.gz"]

        # this may be a bit over the top but what the heck..
        for sn in sample_names:
            for si in sample_index:
                for l in lane_no:
                    for i in is_index:
                        for r in read_no:
                            for sx in suffix:
                                good_filename = "_".join([
                                    sn,
                                    si,
                                    "L00{}".format(str(l)),
                                    "{}{}".format(
                                        "I" if i else "R",
                                        str(r)),
                                    sx])
                                observed_sample_file = self.sample_repo.sample_file_from_sample_path(good_filename)
                                self.assertTrue(all([
                                    sn == observed_sample_file.sample_name,
                                    si == observed_sample_file.sample_index,
                                    l == observed_sample_file.lane_no,
                                    i == observed_sample_file.is_index,
                                    r == observed_sample_file.read_no
                                ]))
