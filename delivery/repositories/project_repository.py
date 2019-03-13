
import logging
import os

from delivery.services.file_system_service import FileSystemService
from delivery.models.project import GeneralProject, RunfolderProject
from delivery.exceptions import TooManyProjectsFound, ProjectNotFoundException

log = logging.getLogger(__name__)


class GeneralProjectRepository(object):
    """
    Repository for a general project. For this purpose a project is represented by any director in
    root directory defined by the configuration.
    """

    def __init__(self, root_directory, filesystem_service=FileSystemService()):
        """
        Instantiate a `GeneralProjectRepository` instance
        :param root_directory: directory in which to look for projects
        :param filesystem_service: a file system service used to interact with the file system, defaults to
        `FileSystemService`
        """
        self.root_directory = root_directory
        self.filesystem_service = filesystem_service

    def get_projects(self):
        """
        TODO
        :return:
        """
        for directory in self.filesystem_service.list_directories(self.root_directory):
            abs_path = self.filesystem_service.abspath(directory)
            yield GeneralProject(name=self.filesystem_service.basename(abs_path),
                                 path=abs_path)

    def get_project(self, project_name):
        """
        TODO
        :param project_name:
        :return:
        """
        known_projects = self.get_projects()
        matching_project = list(filter(lambda p: p.name == project_name, known_projects))

        if not matching_project:
            raise ProjectNotFoundException("Could not find a project with name: {}".format(project_name))
        if len(matching_project) > 1:
            raise TooManyProjectsFound("Found more than one project matching name: {}. This should"
                                       "not be possible...".format(dir()))

        exact_project = matching_project[0]
        return exact_project


class UnorganisedRunfolderProjectRepository(object):

    PROJECTS_DIR = "Unaligned"

    def __init__(self, filesystem_service=FileSystemService()):
        self.filesystem_service = filesystem_service

    def dump_checksums(self, project):

        def _sample_file_checksum(sample_file):
            return [
                sample_file.checksum,
                self.filesystem_service.relpath(
                    sample_file.sample_path,
                    project.path)] if sample_file.checksum else None

        def _sample_checksums(sample):
            for sample_file in sample.sample_files:
                yield _sample_file_checksum(sample_file)

        checksum_path = os.path.join(project.path, "checksums.md5")
        self.filesystem_service.write_checksum_file(
            checksum_path,
            {path: checksum for sample in project.samples for checksum, path in _sample_checksums(sample) if checksum})

        return checksum_path

    def get_projects(self, runfolder, sample_repository):

        def dir_contains_fastq_files(d):
            return any(
                map(
                    lambda f: f.endswith("fastq.gz"),
                    self.filesystem_service.list_files_recursively(d)))

        def project_from_dir(d):
            project = RunfolderProject(
                name=os.path.basename(d),
                path=os.path.join(projects_base_dir, d),
                runfolder_path=runfolder.path,
                runfolder_name=runfolder.name
            )
            project.samples = sample_repository.get_samples(project, runfolder)
            return project

        try:
            projects_base_dir = os.path.join(runfolder.path, self.PROJECTS_DIR)

            # only include directories that have fastq.gz files beneath them
            project_directories = filter(
                dir_contains_fastq_files,
                self.filesystem_service.find_project_directories(projects_base_dir)
            )

            # There are scenarios where there are no project directories in the runfolder,
            # i.e. when fastq files have not yet been divided into projects
            return list(map(project_from_dir, project_directories)) or None

        except FileNotFoundError:
            log.warning("Did not find Unaligned folder for: {}".format(runfolder.name))
            pass
