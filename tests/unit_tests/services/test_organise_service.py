import mock
import os
import unittest

from delivery.exceptions import ProjectAlreadyOrganisedException
from delivery.models.sample import Sample, SampleFile
from delivery.repositories.sample_repository import RunfolderProjectBasedSampleRepository
from delivery.services.file_system_service import FileSystemService
from delivery.services.runfolder_service import RunfolderService
from delivery.services.organise_service import OrganiseService

from tests import test_utils


class TestOrganiseService(unittest.TestCase):

    def setUp(self):
        self.project = test_utils.FAKE_RUNFOLDERS[0].projects[0]
        file_root_path = os.path.join(self.project.runfolder_path, "Unaligned")
        self.samples = [
            Sample("Sample1", self.project.name),
            Sample("Sample2", self.project.name)]
        for sample in self.samples:
            sample.sample_files = [
                SampleFile(
                    os.path.join(file_root_path, "sample_file_lane_{}".format(i)),
                    lane_no=i) for i in range(1, 4)]
            file_root_path = os.path.join(file_root_path, "Sample_{}".format(self.samples[1].name))
        self.file_system_service = mock.MagicMock(spec=FileSystemService)
        self.sample_repository = mock.MagicMock(spec=RunfolderProjectBasedSampleRepository)
        self.runfolder_service = mock.MagicMock(spec=RunfolderService)
        self.organise_service = OrganiseService(
            self.runfolder_service,
            self.sample_repository,
            file_system_service=self.file_system_service)

    def test_organise_runfolder(self):
        self.runfolder_service.find_projects_on_runfolder.side_effect = yield from [self.project]
        with mock.patch.object(self.organise_service, "organise_project", autospec=True) as organise_project_mock:
            runfolder_id = self.project.runfolder_name
            lanes = [1, 2, 3]
            projects = ["a", "b", "c"]
            force = False
            self.organise_service.organise_runfolder(runfolder_id, lanes, projects, force)
            organise_project_mock.assert_called_once_with(self.project, lanes, force)

    def test_organise_project_already_organised(self):
        self.file_system_service.exists.return_value = True

        # without force
        self.assertRaises(
            ProjectAlreadyOrganisedException,
            self.organise_service.organise_project,
            self.project, [], False)

        # with force
        organised_path = os.path.join(self.project.runfolder_path, "Projects")
        self.sample_repository.get_samples.return_value = []
        self.organise_service.organise_project(self.project, [], True)
        self.file_system_service.rename.assert_called_once()
        self.assertEqual(organised_path, self.file_system_service.rename.call_args[0][0])
        self.assertRegex(
            self.file_system_service.rename.call_args[0][1],
            "{}\\.\\d+\\.\\d+".format(organised_path))

    def test_organise_project(self):
        self.sample_repository.get_samples.return_value = self.samples
        organised_project_path = os.path.join(self.project.runfolder_path, "Projects", self.project.name)
        with mock.patch.object(self.organise_service, "organise_sample", autospec=True) as organise_sample_mock:
            lanes = [1, 2, 3]
            force = True
            self.organise_service.organise_project(self.project, lanes, force)
            for sample in self.samples:
                organise_sample_mock.assert_has_calls([
                    mock.call(
                        sample,
                        organised_project_path,
                        lanes)])

    def test_organise_sample(self):

        # relative symlinks should be created with the correct arguments
        self.file_system_service.relpath.side_effect = os.path.relpath
        for sample in self.samples:
            organised_sample = self.organise_service.organise_sample(sample, self.project.path, [])
            sample_file_dir = os.path.relpath(
                os.path.dirname(
                    sample.sample_files[0].sample_path),
                self.project.runfolder_path)
            relative_path = os.path.join("..", "..", "..", sample_file_dir)
            self.file_system_service.symlink.assert_has_calls([
                mock.call(
                    os.path.join(relative_path, os.path.basename(sample_file.sample_path)),
                    sample_file.sample_path) for sample_file in organised_sample.sample_files])

    def test_organise_sample_exclude_by_lane(self):

        # all sample lanes are excluded
        for sample in self.samples:
            organised_sample = self.organise_service.organise_sample(sample, self.project.path, [0])
            self.assertListEqual([], organised_sample.sample_files)

        # a specific sample lane is excluded
        for sample in self.samples:
            organised_sample = self.organise_service.organise_sample(sample, self.project.path, [2, 3])
            self.assertEqual(0, len(list(filter(lambda f: f.lane_no == 1, organised_sample.sample_files))))
            self.assertEqual(2, len(list(filter(lambda f: f.lane_no != 1, organised_sample.sample_files))))

