
import logging
import os
import time

from delivery.exceptions import ProjectAlreadyOrganisedException

from delivery.models.sample import Sample, SampleFile
from delivery.services.file_system_service import FileSystemService

log = logging.getLogger(__name__)


class OrganiseService(object):
    """
    Starting in this context means copying a directory or file to a separate directory before delivering it.
    This service handles that in a asynchronous way. Copying operations (right nwo powered by rsync) can be
    started, and their status monitored by querying the underlying database for their status.
    """

    def __init__(self, runfolder_service, sample_repository, file_system_service=FileSystemService()):
        """
        Instantiate a new StagingService
        :param staging_dir: the directory to which files/dirs should be staged
        :param external_program_service: a instance of ExternalProgramService
        :param staging_repo: a instance of DatabaseBasedStagingRepository
        :param runfolder_repo: a instance of FileSystemBasedRunfolderRepository
        :param project_dir_repo: a instance of GeneralProjectRepository
        :param project_links_directory: a path to a directory where links will be created temporarily
                                        before they are rsynced into staging (for batched deliveries etc)
        :param session_factory: a factory method which can produce new sqlalchemy Session instances
        """
        self.runfolder_service = runfolder_service
        self.sample_repository = sample_repository
        self.file_system_service = file_system_service

    def organise_runfolder(self, runfolder_id, lanes, projects, force):
        projects_on_runfolder = self.runfolder_service.find_projects_on_runfolder(
            runfolder_name=runfolder_id,
            only_these_projects=projects)
        for project in projects_on_runfolder:
            try:
                self.organise_project(project, lanes, force)
            except ProjectAlreadyOrganisedException as e:
                log.info(e)
                log.info("no re-organisation of {} was attempted".format(project.name))

    def organise_project(self, project, lanes, force):
        organised_project_path = os.path.join(project.runfolder_path, "Projects", project.name)

        # handle the case when the organised path already exists
        if self.file_system_service.exists(organised_project_path):
            msg = "Organised project path '{}' already exists".format(organised_project_path)
            if not force:
                raise ProjectAlreadyOrganisedException(msg)
            existing_path = os.path.dirname(organised_project_path)
            backup_path = "{}.{}".format(existing_path, str(time.time()))
            log.info(msg)
            log.info("existing path '{}' will be moved to '{}'".format(existing_path, backup_path))
            self.file_system_service.rename(existing_path, backup_path)

        # symlink the samples
        self.file_system_service.makedirs(organised_project_path)
        samples = self.sample_repository.get_samples(project)
        for sample in samples:
            self.organise_sample(sample, organised_project_path, lanes)

    def organise_sample(self, sample, organised_project_path, lanes):

        def _include_sample_file(f):
            return not lanes or f.lane_no in lanes

        # symlink each sample in its own directory
        organised_sample_path = os.path.join(organised_project_path, "Sample_{}".format(sample.name))

        # filter the files if lanes should be excluded
        sample_files_to_symlink = list(filter(_include_sample_file, sample.sample_files))
        if sample_files_to_symlink:
            self.file_system_service.makedirs(organised_sample_path)

        # symlink the sample files using relative paths
        organised_sample = Sample(
            name=sample.name,
            project_name=sample.project_name,
            sample_files=[])
        for sample_file in sample_files_to_symlink:
            link_name = os.path.join(organised_sample_path, sample_file.file_name)
            relative_path = self.file_system_service.relpath(sample_file.sample_path, os.path.dirname(link_name))
            self.file_system_service.symlink(relative_path, link_name)
            organised_sample.sample_files.append(
                SampleFile(
                    link_name,
                    sample_name=sample_file.sample_name,
                    sample_index=sample_file.sample_index,
                    lane_no=sample_file.lane_no,
                    read_no=sample_file.read_no,
                    is_index=sample_file.is_index,
                    checksum=sample_file.checksum))
        return organised_sample
