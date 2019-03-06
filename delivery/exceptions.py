

class RunfolderNotFoundException(Exception):
    """
    Should be raised when a runfolder is not found
    """
    pass


class ChecksumNotFoundException(Exception):
    pass


class ChecksumFileNotFoundException(Exception):
    pass


class ProjectNotFoundException(Exception):
    """
    Should be raised when and invalid or non-existent project is searched for.
    """
    pass


class TooManyProjectsFound(Exception):
    """
    Should be raise when to many projects match some specific criteria
    """
    pass


class InvalidStatusException(Exception):
    """
    Should be raised when an object is found to be in a invalid state, e.g. if the program tries to start staging
    on a StagingOrder which is already `in_progress`
    """
    pass


class CannotParseMoverOutputException(Exception):
    """
    Should be raised when movers output cannot be parsed for e.g. a mover delivery id.
    """
    pass


class ProjectAlreadyDeliveredException(Exception):
    """
    Should be raised when a project has already been delivered.
    """
    pass


class ProjectAlreadyOrganisedException(Exception):
    """
    Should be raised when a project has already been organised.
    """
    pass


class FileNameParsingException(Exception):
    pass
