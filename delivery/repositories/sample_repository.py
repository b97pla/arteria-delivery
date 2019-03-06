
import logging
import os
import re

from delivery.exceptions import ChecksumNotFoundException, FileNameParsingException
from delivery.models.sample import Sample, SampleFile

from delivery.services.file_system_service import FileSystemService

log = logging.getLogger(__name__)


class RunfolderProjectBasedSampleRepository(object):

    filename_regexp = r'^(.+)_(S\d+)_L00(\d+)_([IR])(\d)_\d+\.fastq\.gz$'

    def __init__(self, file_system_service=FileSystemService()):
        self.file_system_service = file_system_service

    def get_samples(self, project):
        return self._get_samples(project)

    def _get_samples(self, project):

        def _is_fastq_file(f):
            return re.match(self.filename_regexp, f) is not None

        def _name_from_sample_file(s):
            return s.sample_name

        def _sample_from_name(n):
            return Sample(n, project.name)

        project_fastq_files = filter(
            _is_fastq_file,
            self.file_system_service.list_files_recursively(project.path))

        # create SampleFile objects from the paths, create Sample objects and attach the sample file objects
        project_sample_files = list(map(
            self.sample_file_from_sample_path,
            project_fastq_files))
        project_samples = map(
            _sample_from_name,
            set(map(
                _name_from_sample_file,
                project_sample_files)))
        for project_sample in project_samples:
            project_sample.sample_files = list(filter(
                lambda f: f.sample_name == project_sample.name,
                project_sample_files
            ))
            yield project_sample

    def sample_file_from_sample_path(self, sample_path):
        file_name = os.path.basename(sample_path)
        m = re.match(self.filename_regexp, file_name)
        if not m or len(m.groups()) != 5:
            raise FileNameParsingException("Could not parse information from file name '{}'".format(file_name))
        sample_name = str(m.group(1))
        sample_index = str(m.group(2))
        lane_no = int(m.group(3))
        is_index = (str(m.group(4)) == "I")
        read_no = int(m.group(5))
        return SampleFile(
            sample_path,
            sample_name=sample_name,
            sample_index=sample_index,
            lane_no=lane_no,
            read_no=read_no,
            is_index=is_index)

