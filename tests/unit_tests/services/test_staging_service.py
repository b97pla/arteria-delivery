import mock
import signal
import random
import os
import tempfile

from tornado.testing import AsyncTestCase
from tornado.gen import coroutine
import tornado.testing

from delivery.exceptions import InvalidStatusException, RunfolderNotFoundException, ProjectNotFoundException
from delivery.services.staging_service import StagingService
from delivery.services.file_system_service import FileSystemService
from delivery.services.external_program_service import ExternalProgramService
from delivery.models.db_models import StagingOrder, StagingStatus
from delivery.models.execution import Execution, ExecutionResult
from delivery.models.project import GeneralProject
from delivery.models.project import RunfolderProject
from tests.test_utils import FAKE_RUNFOLDERS, assert_eventually_equals, MockIOLoop


class TestStagingService(AsyncTestCase):

    class MockStagingRepo:

        def __init__(self):
            self.orders_state = []

        def get_staging_order_by_id(self, identifier, custom_session=None):
            return list(filter(lambda x: x.id == identifier, self.orders_state))[0]

        def create_staging_order(self, source, status, staging_target_dir):

            order = StagingOrder(id=len(self.orders_state) + 1,
                                 source=source,
                                 status=status,
                                 staging_target=staging_target_dir)
            self.orders_state.append(order)
            return order

    def setUp(self):
        self.staging_order1 = StagingOrder(id=1,
                                           source='/test/this',
                                           staging_target='/foo',
                                           status=StagingStatus.pending)

        self.mock_general_project_repo = mock.MagicMock()

        stdout_mimicing_rsync = """
            Number of files: 1 (reg: 1)
            Number of created files: 0
            Number of deleted files: 0
            Number of regular files transferred: 1
            Total file size: 207,707,566 bytes
            Total transferred file size: 207,707,566 bytes
            Literal data: 207,707,566 bytes
            Matched data: 0 bytes
            File list size: 0
            File list generation time: 0.001 seconds
            File list transfer time: 0.000 seconds
            Total bytes sent: 207,758,378
            Total bytes received: 35

            sent 207,758,378 bytes  received 35 bytes  138,505,608.67 bytes/sec
            total size is 207,707,566  speedup is 1.00
        """

        mock_process = mock.MagicMock()
        mock_execution = Execution(pid=random.randint(1, 1000), process_obj=mock_process)

        self.mock_external_runner_service = mock.create_autospec(ExternalProgramService)
        self.mock_external_runner_service.run.return_value = mock_execution

        self.mock_file_system_service = mock.create_autospec(FileSystemService)

        @coroutine
        def wait_as_coroutine(x):
            return ExecutionResult(stdout=stdout_mimicing_rsync, stderr="", status_code=0)

        self.mock_external_runner_service.wait_for_execution = wait_as_coroutine
        mock_staging_repo = mock.MagicMock()
        mock_staging_repo.get_staging_order_by_id.return_value = self.staging_order1
        mock_staging_repo.create_staging_order.return_value = self.staging_order1

        self.mock_runfolder_repo = mock.MagicMock()

        mock_db_session_factory = mock.MagicMock()

        self.staging_service = StagingService(staging_dir="/tmp",
                                              project_links_directory="/tmp",
                                              external_program_service=self.mock_external_runner_service,
                                              staging_repo=mock_staging_repo,
                                              runfolder_repo=self.mock_runfolder_repo,
                                              session_factory=mock_db_session_factory,
                                              project_dir_repo=self.mock_general_project_repo,
                                              file_system_service=self.mock_file_system_service)
        self.staging_service.io_loop_factory = MockIOLoop
        super(TestStagingService, self).setUp()

    # A StagingService should be able to:
    # - Stage a staging order
    @tornado.testing.gen_test
    def test_stage_order(self):
        res = yield self.staging_service.stage_order(stage_order=self.staging_order1)

        def _get_stating_status():
            return self.staging_order1.status

        def _get_staging_size():
            return self.staging_order1.size

        assert_eventually_equals(self, 1, _get_stating_status, StagingStatus.staging_successful)
        self.assertEqual(self.staging_order1.size, 207707566)

    # - Set status to failed if rsyncing is not successful
    @tornado.testing.gen_test
    def test_unsuccessful_staging_order(self):
        @coroutine
        def wait_as_coroutine(x):
            return ExecutionResult(stdout="", stderr="", status_code=1)

        self.mock_external_runner_service.wait_for_execution = wait_as_coroutine

        yield self.staging_service.stage_order(stage_order=self.staging_order1)

        def _get_stating_status():
            return self.staging_order1.status

        assert_eventually_equals(self, 1, _get_stating_status, StagingStatus.staging_failed)

    # - Set status to failed if there is an exception is not successful
    def test_exception_in_staging_order(self):

        def raise_exception(x):
            raise Exception
        self.mock_external_runner_service.wait_for_execution = raise_exception
        self.staging_service.stage_order(stage_order=self.staging_order1)

        def _get_stating_status():
            return self.staging_order1.status

        assert_eventually_equals(self, 1, _get_stating_status, StagingStatus.staging_failed)

    # - Reject staging order if it has invalid state
    @tornado.testing.gen_test
    def test_stage_order_non_valid_state(self):
        with self.assertRaises(InvalidStatusException):

            staging_order_in_progress = StagingOrder(source='/test/this',
                                                     status=StagingStatus.staging_in_progress)

            res = yield self.staging_service.stage_order(stage_order=staging_order_in_progress)

    # - Be able to get the status of a stage order
    def test_get_status_or_stage_order(self):
        # Returns correctly for existing order
        actual = self.staging_service.get_status_of_stage_order(self.staging_order1.id)
        self.assertEqual(actual, self.staging_order1.status)

        # Returns None for none existent order
        mock_staging_repo = mock.MagicMock()
        mock_staging_repo.get_staging_order_by_id.return_value = None
        self.staging_service.staging_repo = mock_staging_repo
        actual_not_there = self.staging_service.get_status_of_stage_order(1337)
        self.assertIsNone(actual_not_there)

    # - Be able to kill a ongoing staging process
    @mock.patch('delivery.services.staging_service.os')
    def test_kill_stage_order(self, mock_os):

        # If the status is in progress it should be possible to kill it.
        self.staging_order1.status = StagingStatus.staging_in_progress
        self.staging_order1.pid = 1337
        actual = self.staging_service.kill_process_of_staging_order(self.staging_order1.id)
        mock_os.kill.assert_called_with(self.staging_order1.pid, signal.SIGTERM)
        self.assertTrue(actual)

        # It should handle if kill raises a OSError gracefully
        self.staging_order1.status = StagingStatus.staging_in_progress
        self.staging_order1.pid = 1337
        mock_os.kill.side_effect = OSError
        actual = self.staging_service.kill_process_of_staging_order(self.staging_order1.id)
        mock_os.kill.assert_called_with(self.staging_order1.pid, signal.SIGTERM)
        self.assertFalse(actual)

    @mock.patch('delivery.services.staging_service.os')
    def test_kill_stage_order_not_valid_state(self, mock_os):
        # If the status is not in progress it should not be possible to kill it.
        self.staging_order1.status = StagingStatus.staging_successful
        actual = self.staging_service.kill_process_of_staging_order(self.staging_order1.id)
        mock_os.kill.assert_not_called()
        self.assertFalse(actual)
