
import logging

from arteria.web.handlers import BaseRestHandler
from delivery.handlers import OK, INTERNAL_SERVER_ERROR

log = logging.getLogger(__name__)


class BaseOrganiseHandler(BaseRestHandler):
    pass


class OrganiseRunfolderHandler(BaseOrganiseHandler):
    """
    Handler class for handling how to start staging of a runfolder.
    """

    def initialize(self, organise_service, **kwargs):
        self.organise_service = organise_service

    def post(self, runfolder_id):
        """
        Attempt to stage projects from the the specified runfolder, so that they can then be delivered.
        Will return a set of status links, one for each project that can be queried for the status of
        that staging attempt. A list of project names can be specified in the request body to limit which projects
        should be staged. E.g:

            import requests

            url = "http://localhost:8080/api/1.0/stage/runfolder/160930_ST-E00216_0111_BH37CWALXX"

            payload = "{'projects': ['ABC_123']}"
            headers = {
                'content-type': "application/json",
            }

            response = requests.request("POST", url, data=payload, headers=headers)

            print(response.text)

        The return format looks like:
            {"staging_order_links": {"ABC_123": "http://localhost:8080/api/1.0/stage/584"}}

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

