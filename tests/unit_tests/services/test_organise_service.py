import mock
import os
import unittest

from delivery.exceptions import ProjectAlreadyOrganisedException
from delivery.models.project import RunfolderProject
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

    def test_check_previously_organised_project(self):
        organised_project_base_path = os.path.dirname(self.organised_project_path)
        organised_projects_path = os.path.dirname(organised_project_base_path)
        # not previously organised
        self.file_system_service.exists.return_value = False
        self.assertIsNone(
            self.organise_service.check_previously_organised_project(
                self.project,
                organised_projects_path,
                False
            ))
        # previously organised and not forced
        self.file_system_service.exists.return_value = True
        self.assertRaises(
            ProjectAlreadyOrganisedException,
            self.organise_service.check_previously_organised_project,
            self.project,
            organised_projects_path,
            False)
        # previously organised and forced
        self.organise_service.check_previously_organised_project(
            self.project,
            organised_projects_path,
            True)
        self.file_system_service.rename.assert_called_once()

    def test_organise_runfolder_already_organised(self):
        self.file_system_service.exists.return_value = True
        with mock.patch.object(self.organise_service, "organise_project", autospec=True) as organise_project_mock:
            expected_organised_project = "this-is-an-organised-project"
            organise_project_mock.return_value = expected_organised_project
            self.runfolder_service.find_projects_on_runfolder.side_effect = yield from [self.project]
            runfolder_id = self.runfolder.name

            # without force
            self.assertRaises(
                ProjectAlreadyOrganisedException,
                self.organise_service.organise_runfolder,
                runfolder_id, [], [], False)

            # with force
            organised_runfolder = self.organise_service.organise_runfolder(runfolder_id, [], [], True)
            self.assertEqual(self.runfolder.name, organised_runfolder.name)
            self.assertEqual(self.runfolder.path, organised_runfolder.path)
            self.assertEqual(self.runfolder.checksums, organised_runfolder.checksums)
            self.assertListEqual([expected_organised_project], organised_runfolder.projects)

    def test_organise_project(self):
        with mock.patch.object(
                self.organise_service, "organise_sample", autospec=True) as organise_sample_mock, \
                mock.patch.object(
                    self.organise_service, "symlink_project_report", autospec=True) as symlink_report_mock:
            lanes = [1, 2, 3]
            organised_projects_path = os.path.join(self.project.runfolder_path, "Projects")
            self.organise_service.organise_project(self.runfolder, self.project, organised_projects_path, lanes)
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

    def test_organise_sample_file(self):
        lanes = [1, 2, 3, 6, 7, 8]
        self.file_system_service.relpath.side_effect = os.path.relpath
        for sample in self.project.samples:
            for sample_file in sample.sample_files:
                organised_sample_path = os.path.join(
                    os.path.dirname(
                        os.path.dirname(
                            sample_file.sample_path)),
                    "{}_organised".format(sample.sample_id))
                organised_sample_file = self.organise_service.organise_sample_file(
                    sample_file,
                    organised_sample_path,
                    lanes)

                # if the sample file is derived from a lane that should be skipped
                if sample_file.lane_no not in lanes:
                    self.assertIsNone(organised_sample_file)
                    continue

                expected_link_path = os.path.join(
                    organised_sample_path,
                    os.path.basename(sample_file.sample_path))
                self.assertEqual(
                    expected_link_path,
                    organised_sample_file.sample_path)
                self.file_system_service.symlink.assert_called_with(
                    os.path.join(
                        "..",
                        os.path.basename(
                            os.path.dirname(sample_file.sample_path)),
                        os.path.basename(sample_file.sample_path)),
                    expected_link_path)
                for attr in ("file_name", "sample_name", "sample_index", "lane_no", "read_no", "is_index", "checksum"):
                    self.assertEqual(
                        getattr(sample_file, attr),
                        getattr(organised_sample_file, attr))

    def test_symlink_project_report(self):
        organised_project_path = "/bar/project"
        organised_project = RunfolderProject(
            self.project.name,
            organised_project_path,
            self.project.runfolder_path,
            self.project.runfolder_name)
        project_report_base = "/foo"
        project_report_files = [
            os.path.join(project_report_base, "a-report-file"),
            os.path.join(project_report_base, "report-dir", "another-report-file")
        ]
        self.runfolder_service.get_project_report_files.return_value = project_report_base, project_report_files
        self.file_system_service.relpath.side_effect = os.path.relpath
        self.file_system_service.dirname.side_effect = os.path.dirname
        self.organise_service.symlink_project_report(self.project, organised_project)
        self.file_system_service.symlink.assert_has_calls([
            mock.call(
                os.path.join("..", "..", "foo", "a-report-file"),
                os.path.join(organised_project_path, "a-report-file")),
            mock.call(
                os.path.join("..", "..", "..", "foo", "report-dir", "another-report-file"),
                os.path.join(organised_project_path, "report-dir", "another-report-file"))])
