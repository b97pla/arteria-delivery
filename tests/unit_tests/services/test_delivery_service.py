
import random
import unittest
from mock import MagicMock, create_autospec

from delivery.services.external_program_service import ExternalProgramService
from delivery.services.delivery_service import MoverDeliveryService
from delivery.models.db_models import DeliveryOrder, StagingOrder, StagingStatus, DeliveryStatus
from delivery.models.execution import ExecutionResult, Execution
from delivery.exceptions import InvalidStatusException, CannotParseMoverOutputException

from tests.test_utils import MockIOLoop, assert_eventually_equals


class TestMoverDeliveryService(unittest.TestCase):

    def setUp(self):

        example_mover_stdout = """id=TestCase_31-ngi2016001-1484739218 Found receiver delivery00001 with end date: 2017-03-11
                                  TestCase_31 queued for delivery to delivery00001, id = TestCase_31-ngi2016001-1484739218"""

        example_moverinfo_stdout = """Delivered: Jan 19 00:23:31 [1484781811UTC]"""

        mock_mover_runner = create_autospec(ExternalProgramService)
        mock_process = MagicMock()
        mock_execution = Execution(pid=random.randint(1, 1000), process_obj=mock_process)
        mock_mover_runner.run.return_value = mock_execution
        mock_mover_runner.wait_for_execution.return_value = ExecutionResult(stdout=example_mover_stdout,
                                                                            stderr="",
                                                                            status_code=0)

        mock_moverinfo_runner = create_autospec(ExternalProgramService)
        mock_moverinfo_runner.run_and_wait.return_value = ExecutionResult(stdout=example_moverinfo_stdout,
                                                                          stderr="",
                                                                          status_code=0)
        self.mock_staging_service = MagicMock()
        self.mock_delivery_repo = MagicMock()

        self.delivery_order = DeliveryOrder(id=1)

        self.mock_delivery_repo.create_delivery_order.return_value = self.delivery_order
        self.mock_delivery_repo.get_delivery_order_by_id.return_value = self.delivery_order

        self.mock_session_factory = MagicMock()
        self.mover_delivery_service = MoverDeliveryService(external_program_service=None,
                                                           staging_service=self.mock_staging_service,
                                                           delivery_repo=self.mock_delivery_repo,
                                                           session_factory=self.mock_session_factory)

        # Inject separate external runner instances for the tests, since they need to return
        # different information
        self.mover_delivery_service.mover_external_program_service = mock_mover_runner
        self.mover_delivery_service.moverinfo_external_program_service = mock_moverinfo_runner

        self.mover_delivery_service.io_loop_factory = MockIOLoop

    def test_deliver_by_staging_id(self):
        staging_order = StagingOrder(source='/foo/bar', staging_target='/staging/dir/bar')
        staging_order.status = StagingStatus.staging_successful
        self.mock_staging_service.get_stage_order_by_id.return_value = staging_order

        self.mock_staging_service.get_delivery_order_by_id.return_value = self.delivery_order

        self.mover_delivery_service.deliver_by_staging_id(staging_id=1,
                                                          delivery_project='xyz123',
                                                          md5sum_file='md5sum_file')

        def _get_delivery_order():
            return self.delivery_order.delivery_status
        assert_eventually_equals(self, 1, _get_delivery_order, DeliveryStatus.delivery_in_progress)

    def test_update_delivery_status(self):
        delivery_order = DeliveryOrder(mover_delivery_id="TestCase_31-ngi2016001-1484739218 ",
                                       delivery_status=DeliveryStatus.delivery_in_progress)
        self.mock_delivery_repo.get_delivery_order_by_id.return_value = delivery_order
        result = self.mover_delivery_service.update_delivery_status(self.delivery_order.id)
        self.assertEqual(result.delivery_status, DeliveryStatus.delivery_successful)

    def test_deliver_by_staging_id_raises_on_non_existent_stage_id(self):
        self.mock_staging_service.get_stage_order_by_id.return_value = None

        with self.assertRaises(InvalidStatusException):

            self.mover_delivery_service.deliver_by_staging_id(staging_id=1,
                                                              delivery_project='foo',
                                                              md5sum_file='md5sum_file')

    def test_deliver_by_staging_id_raises_on_non_successful_stage_id(self):

        staging_order = StagingOrder()
        staging_order.status = StagingStatus.staging_failed
        self.mock_staging_service.get_stage_order_by_id.return_value = staging_order

        with self.assertRaises(InvalidStatusException):

            self.mover_delivery_service.deliver_by_staging_id(staging_id=1,
                                                              delivery_project='foo',
                                                              md5sum_file='md5sum_file')

    def test_get_status_of_delivery_order(self):
        delivery_order = DeliveryOrder(id=1,
                                       delivery_source='src',
                                       delivery_project='xyz123',
                                       delivery_status=DeliveryStatus.mover_processing_delivery,
                                       staging_order_id=11,
                                       md5sum_file='file')
        self.mock_delivery_repo.get_delivery_order_by_id.return_value = delivery_order
        actual = self.mover_delivery_service.get_status_of_delivery_order(1)
        self.assertEqual(actual, DeliveryStatus.mover_processing_delivery)

    def test_delivery_order_by_id(self):
        delivery_order = DeliveryOrder(id=1,
                                       delivery_source='src',
                                       delivery_project='xyz123',
                                       delivery_status=DeliveryStatus.mover_processing_delivery,
                                       staging_order_id=11,
                                       md5sum_file='file')
        self.mock_delivery_repo.get_delivery_order_by_id.return_value = delivery_order
        actual = self.mover_delivery_service.get_delivery_order_by_id(1)
        self.assertEqual(actual.id, 1)

    def test_possible_to_delivery_by_staging_id_and_skip_mover(self):

        staging_order = StagingOrder(source='/foo/bar', staging_target='/staging/dir/bar')
        staging_order.status = StagingStatus.staging_successful
        self.mock_staging_service.get_stage_order_by_id.return_value = staging_order

        self.mock_staging_service.get_delivery_order_by_id.return_value = self.delivery_order

        self.mover_delivery_service.deliver_by_staging_id(staging_id=1,
                                                          delivery_project='xyz123',
                                                          md5sum_file='md5sum_file',
                                                          skip_mover=True)

        def _get_delivery_order():
            return self.delivery_order.delivery_status
        assert_eventually_equals(self, 1, _get_delivery_order, DeliveryStatus.delivery_skipped)

    def test__parse_mover_id_from_mover_output(self):
        example_mover_output = """id=TestCase_31-ngi2016001-1484739218 Found receiver delivery00001 with end date: 2017-03-11
                                  TestCase_31 queued for delivery to delivery00001, id = TestCase_31-ngi2016001-1484739218"""

        actual = self.mover_delivery_service._parse_mover_id_from_mover_output(example_mover_output)
        self.assertEqual(actual, "TestCase_31-ngi2016001-1484739218")

    def test__parse_mover_id_from_mover_output_raises_on_invalid_output(self):
        example_mover_output = """Found receiver delivery00001 with end date: 2017-03-11
                                  TestCase_31 queued for delivery to delivery00001, id = TestCase_31-ngi2016001-1484739218"""
        with self.assertRaises(CannotParseMoverOutputException):
            self.mover_delivery_service._parse_mover_id_from_mover_output(example_mover_output)

    def test__parse_status_from_mover_info_result(self):
        example_moverinfo_stdout = """Delivered: Jan 19 00:23:31 [1484781811UTC]"""
        actual = self.mover_delivery_service._parse_status_from_mover_info_result(example_moverinfo_stdout)
        self.assertEqual(actual, "Delivered")

    def test__parse_status_from_mover_info_result_raises_on_invalid_output(self):
        with self.assertRaises(CannotParseMoverOutputException):
            self.mover_delivery_service._parse_status_from_mover_info_result("Invalid input...")
