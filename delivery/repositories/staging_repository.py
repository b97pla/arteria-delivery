
import os
import logging

from sqlalchemy.orm.exc import NoResultFound

from delivery.models.db_models import StagingOrder
from delivery.services.file_system_service import FileSystemService

log = logging.getLogger(__name__)


class DatabaseBasedStagingRepository(object):
    """
    A repository of staging orders backed by a database. It is able to create and commit new staging orders
    to the database, and fetch them based on different factors.
    """

    def __init__(self, session_factory, file_system_service=FileSystemService()):
        """
        Instantiate a new DatabaseBasedStagingRepository
        :param session_factory: factory method which can produce new sqlalchemy Session objects
        :param file_system_service: a service for accessing the file system. Mostly shadows normal
                                    stdlib methods for accessing the file system, but this allows for easier mocking
                                    in tests.
        """
        self.session = session_factory()
        self.file_system_service = file_system_service

    def get_staging_order_by_source(self, source):
        """
        Get all staging orders based on their source
        :param source: to search for
        :return: All staging orders of that source as a list
        """
        return self.session.query(StagingOrder).filter(StagingOrder.source == source).all()

    def get_staging_order_by_id(self, identifier, custom_session=None):
        """
        Get a staging order by id
        NOTE
        The custom_session used here is used because in the `StagingService` it is necessary to
        make this query from a separate thread, something which sqlalchemy does not allow. Therefore
        I've made it possible to provide a separate session here (in that case a new session instantiate in the
        thread), while hacky it appears to work. /JD 20161108
        :param identifier: the stating order id to search for
        :param custom_session: provide an other session object if that is neccessary for your use case.
        :return: the matching StagingOrder or None, if there was no matching stating order.
        """
        if custom_session:
            session = custom_session
        else:
            session = self.session
        try:
            return session.query(StagingOrder).filter(StagingOrder.id == identifier).one()
        except NoResultFound:
            return None

    def create_staging_order(self, source, status, staging_target_dir, project_name):
        """
        Create a StatingOrder and commit it to the database
        :param source: the directory or file to stage
        :param status: the initial StatingStatus to assign to the StatingORder
        :param staging_target_dir: the directory to which the StagingOrder should transfer the source
        :param project_name: name of the project to stage (this will be used to determine the name of the
        staging target)
        :return:
        """

        order = StagingOrder(source=source, status=status)
        self.session.add(order)

        self.session.commit()

        if self.file_system_service.isfile(order.source):
            log.debug("Order source is a file")
        elif self.file_system_service.isdir(order.source):
            log.debug("Order source is a dir")
        else:
            raise NotImplementedError("Could not parse a valid type from: {}, valid types"
                                      " are directory and file.".format(order.source))

        staging_target = os.path.join(staging_target_dir, str(order.id), project_name)

        log.debug("Set the staging target to: {}".format(staging_target))

        order.staging_target = staging_target
        self.session.commit()

        return order
