import mock
import os
import unittest

from delivery.exceptions import ProjectAlreadyOrganisedException
from delivery.repositories.project_repository import GeneralProjectRepository
from delivery.repositories.sample_repository import RunfolderProjectBasedSampleRepository
from delivery.services.file_system_service import FileSystemService
from delivery.services.runfolder_service import RunfolderService
from delivery.services.organise_service import OrganiseService

from tests import test_utils


class TestOrganiseService(unittest.TestCase):

    def setUp(self):
        self.runfolder = test_utils.UNORGANISED_RUNFOLDER
        self.project = self.runfolder.projects[0]
        self.organised_project_path = os.path.join(
            self.project.runfolder_path,
            "Projects",
            self.project.name,
            self.runfolder.name)
        self.file_system_service = mock.MagicMock(spec=FileSystemService)
        self.runfolder_service = mock.MagicMock(spec=RunfolderService)
        self.project_repository = mock.MagicMock(spec=GeneralProjectRepository)
        self.sample_repository = mock.MagicMock(spec=RunfolderProjectBasedSampleRepository)
        self.organise_service = OrganiseService(
            self.runfolder_service,
            file_system_service=self.file_system_service)

    def test_organise_runfolder(self):
        self.runfolder_service.find_projects_on_runfolder.side_effect = yield from [self.project]
        with mock.patch.object(self.organise_service, "organise_project", autospec=True) as organise_project_mock:
            runfolder_id = self.runfolder.name
            lanes = [1, 2, 3]
            projects = ["a", "b", "c"]
            force = False
            self.organise_service.organise_runfolder(runfolder_id, lanes, projects, force)
            organise_project_mock.assert_called_once_with(self.runfolder, self.project, lanes, force)

    def test_organise_project_already_organised(self):
        self.file_system_service.exists.return_value = True
        with mock.patch.object(self.organise_service, "symlink_project_report", autospec=True) as symlink_report_mock:
            # without force
            self.assertRaises(
                ProjectAlreadyOrganisedException,
                self.organise_service.organise_project,
                self.runfolder, self.project, [], False)

            # with force
            organised_path = os.path.join(self.project.runfolder_path, "Projects")
            self.sample_repository.get_samples.return_value = []
            self.organise_service.organise_project(self.runfolder, self.project, [], True)
            self.file_system_service.rename.assert_called_once()
            self.assertEqual(organised_path, self.file_system_service.rename.call_args[0][0])
            self.assertRegex(
                self.file_system_service.rename.call_args[0][1],
                "{}\\.\\d+\\.\\d+".format(organised_path))
            self.assertEqual(1, symlink_report_mock.call_count)

    def test_organise_project(self):
        with mock.patch.object(
                self.organise_service, "organise_sample", autospec=True) as organise_sample_mock, \
                mock.patch.object(
                    self.organise_service, "symlink_project_report", autospec=True) as symlink_report_mock:
            lanes = [1, 2, 3]
            force = True
            self.organise_service.organise_project(self.runfolder, self.project, lanes, force)
            for sample in self.project.samples:
                organise_sample_mock.assert_has_calls([
                    mock.call(
                        sample,
                        self.organised_project_path,
                        lanes)])
            self.assertEqual(1, symlink_report_mock.call_count)

    def test_organise_sample(self):
        # relative symlinks should be created with the correct arguments
        self.file_system_service.relpath.side_effect = os.path.relpath
        self.file_system_service.dirname.side_effect = os.path.dirname
        for sample in self.project.samples:
            organised_sample = self.organise_service.organise_sample(sample, self.organised_project_path, [])
            sample_file_dir = os.path.relpath(
                os.path.dirname(
                    sample.sample_files[0].sample_path),
                self.project.runfolder_path)
            relative_path = os.path.join("..", "..", "..", "..", sample_file_dir)
            self.file_system_service.symlink.assert_has_calls([
                mock.call(
                    os.path.join(relative_path, os.path.basename(sample_file.sample_path)),
                    sample_file.sample_path) for sample_file in organised_sample.sample_files])

    def test_organise_sample_exclude_by_lane(self):

        # all sample lanes are excluded
        for sample in self.project.samples:
            organised_sample = self.organise_service.organise_sample(sample, self.organised_project_path, [0])
            self.assertListEqual([], organised_sample.sample_files)

        # a specific sample lane is excluded
        for sample in self.project.samples:
            organised_sample = self.organise_service.organise_sample(sample, self.organised_project_path, [2, 3])
            self.assertListEqual(
                list(map(lambda x: x.file_name, filter(lambda f: f.lane_no in [2, 3], sample.sample_files))),
                list(map(lambda x: x.file_name, organised_sample.sample_files)))

