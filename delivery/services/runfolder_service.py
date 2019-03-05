
import logging

from delivery.exceptions import RunfolderNotFoundException, ProjectNotFoundException


log = logging.getLogger(__name__)


class RunfolderService(object):

    def __init__(self, runfolder_repo):
        self.runfolder_repo = runfolder_repo

    def find_runfolder(self, runfolder_id):
        runfolder = self.runfolder_repo.get_runfolder(runfolder_id)

        if not runfolder:
            raise RunfolderNotFoundException(
                "Couldn't find runfolder matching: {}".format(runfolder_id))
        else:
            return runfolder

    def _validate_project_lists(self, projects_on_runfolder, projects_to_stage):
        projects_to_stage_set = set(projects_to_stage)
        projects_on_runfolder_set = set(projects_on_runfolder)
        return projects_to_stage_set.issubset(projects_on_runfolder_set)

    def find_projects_on_runfolder(self, runfolder_name, only_these_projects=None):
        runfolder = self.find_runfolder(runfolder_name)

        names_of_project_on_runfolder = list(map(lambda x: x.name, runfolder.projects))

        # If no projects have been specified, get all projects
        if only_these_projects:
            projects_to_return = only_these_projects
        else:
            projects_to_return = names_of_project_on_runfolder

        log.debug("Projects to stage: {}".format(projects_to_return))

        if not self._validate_project_lists(names_of_project_on_runfolder, projects_to_return):
            raise ProjectNotFoundException("Projects to stage: {} do not match projects on runfolder: {}".
                                           format(projects_to_return, names_of_project_on_runfolder))

        for project in runfolder.projects:
            if project.name in projects_to_return:
                yield project

    def find_runfolders_for_project(self, project_name):
        return self.runfolder_repo.get_project(project_name=project_name)
