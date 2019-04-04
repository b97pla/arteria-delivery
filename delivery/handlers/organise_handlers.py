
import logging

from arteria.web.handlers import BaseRestHandler
from delivery.handlers import OK, INTERNAL_SERVER_ERROR

log = logging.getLogger(__name__)


class BaseOrganiseHandler(BaseRestHandler):
    pass


class OrganiseRunfolderHandler(BaseOrganiseHandler):
    """
    Handler class for handling how to organise a runfolder in preparation for staging and delivery
    """

    def initialize(self, organise_service, **kwargs):
        self.organise_service = organise_service

    def post(self, runfolder_id):
        """
        Attempt to organise projects from the the specified runfolder, so that they can then be staged and delivered.
        A list of project names and/or lane numbers can be specified in the request body to limit which projects
        and lanes should be organised. A force flag indicating that previously organised projects should be replaced
        can also be specified. E.g:

            import requests

            url = "http://localhost:8080/api/1.0/organise/runfolder/160930_ST-E00216_0111_BH37CWALXX"

            payload = "{'projects': ['ABC_123'], 'lanes': [1, 2, 4], 'force': True}"
            headers = {
                'content-type': "application/json",
            }

            response = requests.request("POST", url, data=payload, headers=headers)

            print(response.text)

        The return format looks like:
            {"organised_path": "/path/to/organised/runfolder/160930_ST-E00216_0111_BH37CWALXX"}

        """

        log.debug("Trying to organise runfolder with id: {}".format(runfolder_id))

        try:
            request_data = self.body_as_object()
        except ValueError:
            request_data = {}

        force = request_data.get("force", False)
        lanes = request_data.get("lanes", [])
        projects = request_data.get("projects", [])

        if any([force, lanes, projects]):
            log.debug(
                "Got the following 'force', 'lanes' and 'projects' attributes to organise: {}".format(
                    [force, lanes, projects]))

        try:
            organised_runfolder = self.organise_service.organise_runfolder(runfolder_id, lanes, projects, force)

            self.set_status(OK)
            self.write_json({"organised_path": organised_runfolder.path})

        except Exception as e:
            self.set_status(INTERNAL_SERVER_ERROR, reason=str(e))

