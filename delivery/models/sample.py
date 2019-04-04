
import os


class Sample(object):
    """
    Models the concept of a sample on disk
    """

    def __init__(self, name, project_name, sample_id=None, sample_files=None):
        """
        Instantiate a new `Sample` object.

        :param name: the sample name, typically used as a prefix in the sample file name
        :param project_name: the name of the project the sample belongs to
        :param sample_id: the sample id, can be different from sample name but is typically the same
        :param sample_files: a list of SampleFile instances representing sequence files belonging to the sample
        """
        self.name = name
        self.sample_id = sample_id or self.name
        self.project_name = project_name
        self.sample_files = sample_files

    def __eq__(self, other):
        return other.name == self.name and \
               other.sample_id == self.sample_id and \
               other.project_name == self.project_name and \
               other.sample_files == self.sample_files


class SampleFile(object):
    """
    Models the concept of a sequence file belonging to a sample
    """

    def __init__(
            self,
            sample_path,
            sample_name=None,
            sample_index=None,
            lane_no=None,
            read_no=None,
            is_index=None,
            checksum=None):
        """
        Instantiate a new `SampleFile` object

        :param sample_path: the path to the file
        :param sample_name: the name of the sample
        :param sample_index: the sample index designator
        :param lane_no: the lane number the sequences in the file were derived from
        :param read_no: the read number
        :param is_index: if True, the sequence file contains index sequences
        :param checksum: the MD5 checksum for this SampleFile
        """
        self.sample_path = os.path.abspath(sample_path)
        self.file_name = os.path.basename(sample_path)
        self.sample_name = sample_name
        self.sample_index = sample_index
        self.lane_no = lane_no
        self.read_no = read_no
        self.is_index = is_index
        self.checksum = checksum

    def __eq__(self, other):
        return other.sample_path == self.sample_path and other.checksum == self.checksum
