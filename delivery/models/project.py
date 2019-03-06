import os

from delivery.models import BaseModel


class BaseProject(BaseModel):
    """
    Base class for the different project models
    """

    def __eq__(self, other):
        """
        Two project should be considered the same if the represent the same directory on disk
        :param other: instance of RunfolderProject
        :return: true if the same project, otherwise false
        """
        if isinstance(other, self.__class__):
            return self.path == other.path
        return False


class RunfolderProject(BaseProject):
    """
    Model a project directory in a runfolder on disk. Note that this means that this project model only extends
    to the idea of projects as subdirectories in a demultiplexed Illumina runfolder.
    """

    def __init__(self, name, path, runfolder_path, runfolder_name, samples=None):
        """
        Instantiate a new `RunfolderProject` object
        :param name: of the project
        :param path: path to the project
        :param runfolder_path: path the runfolder in which this project is stored.
        :param runfolder_name: name of the runfolder in which this project is stored
        """
        self.name = name
        self.path = os.path.abspath(path)
        self.runfolder_path = runfolder_path
        self.runfolder_name = runfolder_name
        self.samples = samples

    def to_dict(self):
        return {"name": self.name,
                "path": self.path,
                "runfolder_path": self.runfolder_path,
                "runfolder_name": self.runfolder_name,
                "samples": self.samples}


class GeneralProject(BaseProject):
    """
    Model representing a project as a directory on disk.
    """

    def __init__(self, name, path):
        """
        Instantiate a new `GeneralProject` object
        :param name: of the project
        :param path: path to the project
        """
        self.name = name
        self.path = os.path.abspath(path)
