
import logging
import os
import time

from delivery.exceptions import ProjectAlreadyOrganisedException

from delivery.models.project import RunfolderProject
from delivery.models.runfolder import Runfolder
from delivery.models.sample import Sample, SampleFile
from delivery.services.file_system_service import FileSystemService

log = logging.getLogger(__name__)


class OrganiseService(object):
    """
    Starting in this context means organising a runfolder in preparation for a delivery. Each project on the runfolder
    will be organised into its own separate directory. Sequence and report files will be symlinked from their original
    location.
    This service handles that in a synchronous way.
    """

    def __init__(self, runfolder_service, file_system_service=FileSystemService()):
        """
        Instantiate a new OrganiseService
        :param runfolder_service: an instance of a RunfolderService
        :param file_system_service: an instance of FileSystemService
        """
        self.runfolder_service = runfolder_service
        self.file_system_service = file_system_service

    def organise_runfolder(self, runfolder_id, lanes, projects, force):
        """
        Organise a runfolder in preparation for delivery. This will create separate subdirectories for each of the
        projects and symlink all files belonging to the project to be delivered under this directory.

        :param runfolder_id: the name of the runfolder to be organised
        :param lanes: if not None, only samples on any of the specified lanes will be organised
        :param projects: if not None, only projects in this list will be organised
        :param force: if True, a previously organised project will be renamed with a unique suffix
        :raises ProjectAlreadyOrganisedException: if project has already been organised and force is False
        :return: a Runfolder instance representing the runfolder after organisation
        """
        runfolder = self.runfolder_service.find_runfolder(runfolder_id)
        projects_on_runfolder = self.runfolder_service.find_projects_on_runfolder(
            runfolder,
            only_these_projects=projects)
        organised_projects = []
        for project in projects_on_runfolder:
            try:
                organised_projects.append(
                    self.organise_project(
                        runfolder,
                        project,
                        lanes,
                        force))
            except ProjectAlreadyOrganisedException as e:
                log.info(e)
                log.info("no re-organisation of {} was attempted".format(project.name))
                raise
        return Runfolder(
            runfolder.name,
            runfolder.path,
            projects=organised_projects)

    def organise_project(self, runfolder, project, lanes, force):
        """
        Organise a project on a runfolder into its own directory and into a standard structure. If the project has
        already been organised, a ProjectAlreadyOrganisedException will be raised, unless force is True. If force is
        True, the existing project path will be renamed with a unique suffix.

        :param runfolder: a Runfolder instance representing the runfolder on which the project belongs
        :param project: a Project instance representing the project to be organised
        :param lanes: if not None, only samples on any of the specified lanes will be organised
        :param force: if True, a previously organised project will be renamed with a unique suffix
        :raises ProjectAlreadyOrganisedException: if project has already been organised and force is False
        :return: a Project instance representing the project after organisation
        """
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
        organised_project_runfolder_path = os.path.join(organised_project_path, runfolder.name)
        self.file_system_service.makedirs(organised_project_runfolder_path)
        organised_samples = []
        for sample in project.samples:
            organised_samples.append(
                self.organise_sample(
                    sample,
                    organised_project_runfolder_path,
                    lanes))
        organised_project = RunfolderProject(
            project.name,
            organised_project_path,
            runfolder.path,
            runfolder.name,
            samples=organised_samples)
        self.symlink_project_report(project, organised_project)
        self.runfolder_service.dump_project_checksums(organised_project)
        self.runfolder_service.dump_project_samplesheet(runfolder, organised_project)
        return organised_project

    def symlink_project_report(self, project, organised_project):
        """
        Find and symlink the project report to the organised project directory.

        :param project: a Project instance representing the project before organisation
        :param organised_project: a Project instance representing the project after organisation
        """
        project_report_base, project_report_files = self.runfolder_service.get_project_report_files(project)

        def _link_name(target_file):
            return os.path.join(
                organised_project.path,
                self.file_system_service.relpath(
                    target_file,
                    project_report_base))

        def _link_path(target_file, link_name):
            return self.file_system_service.relpath(
                target_file,
                self.file_system_service.dirname(link_name)
            )

        for project_report_file in project_report_files:
            link_name = _link_name(project_report_file)
            link_path = _link_path(project_report_file, link_name)
            self.file_system_service.symlink(link_path, link_name)

    def organise_sample(self, sample, organised_project_path, lanes):
        """
        Organise a sample into its own directory under the corresponding project directory. Samples can be excluded
        from organisation based on which lane they were run on. The sample directory will be named identically to the
        sample id field. This may be different from the sample name field which is used as a prefix in the file name
        for the sample files. This is the same behavior as e.g. bcl2fastq uses for sample id and sample name.

        :param sample: a Sample instance representing the sample to be organised
        :param organised_project_path: the path to the organised project directory under which to place the sample
        :param lanes: if not None, only samples run on the any of the specified lanes will be organised
        :return: a new Sample instance representing the sample after organisation
        """

        def _include_sample_file(f):
            return not lanes or f.lane_no in lanes

        # symlink each sample in its own directory
        organised_sample_path = os.path.join(organised_project_path, sample.sample_id)

        # filter the files if lanes should be excluded
        sample_files_to_symlink = list(filter(_include_sample_file, sample.sample_files))
        if sample_files_to_symlink:
            self.file_system_service.makedirs(organised_sample_path)

        # symlink the sample files using relative paths
        organised_sample_files = []
        for sample_file in sample_files_to_symlink:
            link_name = os.path.join(organised_sample_path, sample_file.file_name)
            relative_path = self.file_system_service.relpath(
                sample_file.sample_path,
                self.file_system_service.dirname(link_name))
            self.file_system_service.symlink(relative_path, link_name)
            organised_sample_files.append(
                SampleFile(
                    link_name,
                    sample_name=sample_file.sample_name,
                    sample_index=sample_file.sample_index,
                    lane_no=sample_file.lane_no,
                    read_no=sample_file.read_no,
                    is_index=sample_file.is_index,
                    checksum=sample_file.checksum))
        return Sample(
            name=sample.name,
            project_name=sample.project_name,
            sample_id=sample.sample_id,
            sample_files=organised_sample_files)
