
import os


class Sample(object):

    def __init__(self, name, project_name, sample_files=None):
        self.name = name
        self.project_name = project_name
        self.sample_files = sample_files


class SampleFile(object):

    def __init__(self, sample_path, sample_name=None, sample_index=None, lane_no=None, read_no=None, is_index=None):
        self.sample_path = os.path.abspath(sample_path)
        self.file_name = os.path.basename(sample_path)
        self.sample_name = sample_name
        self.sample_index = sample_index
        self.lane_no = lane_no
        self.read_no = read_no
        self.is_index = is_index
