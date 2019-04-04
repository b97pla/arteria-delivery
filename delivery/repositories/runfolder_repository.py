
import logging
import os
import re

from delivery.exceptions import ChecksumFileNotFoundException
from delivery.models.runfolder import Runfolder
from delivery.models.project import RunfolderProject
from delivery.services.file_system_service import FileSystemService

log = logging.getLogger(__name__)


class FileSystemBasedRunfolderRepository(object):
    """
    Uses the file system as a source of truth for information about what runfolders are available.
    """

    CHECKSUM_FILE_PATH = os.path.join("MD5", "checksums.md5")
    SAMPLESHEET_PATH = "SampleSheet.csv"

    def __init__(self, base_path, file_system_service=FileSystemService()):
        """
        Instantiate a new FileSystemBasedRunfolderRepository
        :param base_path: the directory where runfolders are stored
        :param file_system_service: a service which can access the file system.
        """
        self._base_path = base_path
        self.file_system_service = file_system_service

    def _add_projects_to_runfolder(self, runfolder):
        """
        Will take the given runfolder and mutate the `projects` field.
        If there are no projects found it will leave `projects` as None.
        :param runfolder: to add projects to
        :return: None
        """
        try:
            projects_base_dir = os.path.join(runfolder.path, "Projects")
            project_directories = self.file_system_service.find_project_directories(
                projects_base_dir)

            def project_from_dir(d):
                return RunfolderProject(
                    name=os.path.basename(d),
                    path=os.path.join(projects_base_dir, d),
                    runfolder_path=runfolder.path,
                    runfolder_name=runfolder.name
                )

            # There are scenarios where there are no project directories in the runfolder,
            # i.e. when fastq files have not yet been divided into projects
            if project_directories:
                runfolder.projects = list(map(
                    project_from_dir, project_directories))
        except FileNotFoundError as e:
            log.warning("Did not find Project folder for: {}".format(runfolder.name))
            pass

    def _add_checksums_for_runfolder(self, runfolder):
        checksums = self.file_system_service.parse_checksum_file(self.checksum_file(runfolder))
        runfolder.checksums = checksums

    def _get_runfolders(self):
        # TODO Filter based on expression for runfolders...
        runfolder_expression = r"^\d+_"

        directories = self.file_system_service.find_runfolder_directories(self._base_path)
        for directory in directories:
            if re.match(runfolder_expression, os.path.basename(directory)):

                name = os.path.basename(directory)
                path = os.path.join(self._base_path, directory)

                runfolder = Runfolder(name=name, path=path, projects=None)
                self._add_projects_to_runfolder(runfolder)
                try:
                    self._add_checksums_for_runfolder(runfolder)
                except ChecksumFileNotFoundException as e:
                    log.info(e)

                yield runfolder

    def get_runfolders(self):
        """
        Get all runfolders
        :return: a generator of known runfolders
        """
        return self._get_runfolders()

    def get_runfolder(self, runfolder):
        """
        Get a Runfolder object matching the specified name
        :param runfolder: to look for
        :return: the matching runfolder, or None if no match
        :raises: a AssertionError if more than one runfolder was found
                matching the given name.
        """
        runfolders = self.get_runfolders()
        matching_name = list([r for r in runfolders if r.name == runfolder])
        if len(matching_name) > 1:
            raise AssertionError("Found more than 1 runfolder matching: ".format(r))
        if len(matching_name) > 0 and matching_name[0]:
            return matching_name[0]
        else:
            return None

    def get_projects(self):
        """
        Pick up all projects
        :return: a generator of project instances
        """
        for runfolder in self.get_runfolders():
            if runfolder.projects:
                for project in runfolder.projects:
                    yield project

    def get_project(self, project_name):
        for project in self.get_projects():
            if project.name == project_name:
                yield project

    def dump_project_checksums(self, project):
        raise NotImplementedError()

    def samplesheet_file(self, runfolder):
        return os.path.join(runfolder.path, self.SAMPLESHEET_PATH)

    def checksum_file(self, runfolder):
        return os.path.join(runfolder.path, self.CHECKSUM_FILE_PATH)

    def get_samplesheet(self, runfolder):
        return self.file_system_service.extract_samplesheet_data(self.samplesheet_file(runfolder))


class FileSystemBasedUnorganisedRunfolderRepository(FileSystemBasedRunfolderRepository):
    """
    A subclass of `FileSystemBasedRunfolderRepository` providing functionality for a unorganised runfolder
    """

    def __init__(self, base_path, project_repository, file_system_service=FileSystemService()):
        """
        Instantiate a new `FileSystemBasedUnorganisedRunfolderRepository` object.

        :param base_path: the directory where runfolders are stored
        :param project_repository: an instance of UnorganisedRunfolderProjectRepository
        :param file_system_service: a service which can access the file system
        """
        super(FileSystemBasedUnorganisedRunfolderRepository, self).__init__(
            base_path,
            file_system_service=file_system_service)
        self.project_repository = project_repository

    def _add_projects_to_runfolder(self, runfolder):
        runfolder.projects = self.project_repository.get_projects(runfolder)

    def dump_project_checksums(self, project):
        """
        Calls the `UnorganisedRunfolderProjectRepository` instance associated with this repository to dump out
        checksums for files relevant to the supplied project to a file under the project path.

        :param project: an instance of Project
        :return: the path to the created checksum file
        """
        return self.project_repository.dump_checksums(project)

    def dump_project_samplesheet(self, runfolder, project):
        """
        Parses the SampleSheet from the supplied runfolder and extracts the rows in the [Data] section relevant to
        the samples in the supplied project. The extracted data are written to a samplesheet file under the project
        path.

        :param runfolder: an instance of Runfolder
        :param project: an instance of Project
        :return: the path to the created SampleSheet file
        """
        def _samplesheet_entry_in_project(e):
            """
            Checks if a samplesheet row matches the project w.r.t.:
                * project name
                * sample id in project
                * lane in project samples
            """
            return self.project_repository.is_sample_in_project(
                project,
                e.get("Sample_Project"),
                e.get("Sample_ID"),
                int(e.get("Lane")))

        samplesheet_data = self.get_samplesheet(runfolder)
        project_samplesheet_data = list(filter(_samplesheet_entry_in_project, samplesheet_data))
        project_samplesheet_file = os.path.join(project.path, self.SAMPLESHEET_PATH)
        self.file_system_service.write_samplesheet_file(project_samplesheet_file, project_samplesheet_data)
        return project_samplesheet_file

    def get_project_report_files(self, project):
        """
        Calls the `UnorganisedRunfolderProjectRepository` instance associated with this repository to collect
        paths to report files relevant to the supplied project.

        :param project: an instance of Project
        :return: a tuple with the path to the directory containing the report and a list of paths to the report files
        """
        return self.project_repository.get_report_files(project)
