
import os

from delivery.services.file_system_service import FileSystemService
from delivery.models.project import GeneralProject
from delivery.exceptions import TooManyProjectsFound, ProjectNotFoundException


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
