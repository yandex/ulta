import logging
import pytest
from unittest.mock import MagicMock
from ulta.common.cancellation import Cancellation
from ulta.common.state import State
from ulta.service.status_reporter import StatusReporter
from ulta.service.tank_client import TankStatus
from ulta.yc.ycloud import JWTError
from google.api_core.exceptions import FailedPrecondition, NotFound, Unauthenticated, Unauthorized


def test_report_tank_status():
    tank_client = MagicMock()
    tank_client.get_tank_status.return_value = TankStatus.TANK_FAILED
    loadtesting_client = MagicMock()
    reporter = StatusReporter(logging.getLogger(), tank_client, loadtesting_client, Cancellation(), State())
    reporter.report_tank_status()
    loadtesting_client.claim_tank_status.assert_called_with(TankStatus.TANK_FAILED.name, None)


@pytest.mark.parametrize(
    'exception, expected_exception',
    [
        (JWTError('jwt exception'), Unauthenticated),
        (FailedPrecondition('agent is misconfigured'), FailedPrecondition),
        (NotFound('agent not found'), NotFound),
        (Unauthenticated('unvalid user id'), Unauthenticated),
        (Unauthorized('unauthorized'), Unauthorized),
    ],
)
def test_report_tank_stops_on_exceptions(exception, expected_exception):
    loadtesting_client = MagicMock()
    loadtesting_client.claim_tank_status.side_effect = exception
    cancellation = Cancellation()
    reporter = StatusReporter(logging.getLogger(), MagicMock(), loadtesting_client, cancellation, State())
    with pytest.raises(expected_exception):
        reporter.report_tank_status(TankStatus.TANK_FAILED)
    assert not cancellation.is_set()
    with reporter.run() as stopper:
        assert stopper is not None
        stopper.wait(3)
    assert cancellation.is_set()
    assert stopper.is_set()
    loadtesting_client.claim_tank_status.assert_called_with(
        TankStatus.STOPPED.name,
        "The backend doesn't know this agent: agent has been deleted or account is missing loadtesting.generatorClient role.",
    )


@pytest.mark.parametrize(
    'tank_status_arg, status_message_arg, expected_status, expected_message',
    [
        (TankStatus.READY_FOR_TEST, '', TankStatus.ERROR.name, 'Unable to read work dir'),
        (TankStatus.STOPPED, '', TankStatus.ERROR.name, 'Unable to read work dir'),
        (TankStatus.READY_FOR_TEST, 'Some non-error-thing', TankStatus.ERROR.name, 'Unable to read work dir'),
        (TankStatus.STOPPED, 'Some non-error-thing', TankStatus.ERROR.name, 'Unable to read work dir'),
        (TankStatus.TESTING, '', TankStatus.TESTING.name, ''),
        (TankStatus.TESTING, 'Some non-error-thing', TankStatus.TESTING.name, 'Some non-error-thing'),
        (TankStatus.PREPARING_TEST, '', TankStatus.PREPARING_TEST.name, ''),
        (TankStatus.UPLOADING_ARTIFACTS, '', TankStatus.UPLOADING_ARTIFACTS.name, ''),
    ],
)
def test_report_error_status(tank_status_arg, status_message_arg, expected_status, expected_message):
    loadtesting_client = MagicMock()
    tank_client = MagicMock()
    tank_client.get_tank_status.return_value = tank_status_arg
    cancellation = Cancellation()
    service_state = State()
    service_state.error('stage', OSError('Unable to read work dir'))

    reporter = StatusReporter(logging.getLogger(), tank_client, loadtesting_client, cancellation, service_state)
    reporter.report_tank_status(tank_status_arg, status_message_arg)

    loadtesting_client.claim_tank_status.assert_called_with(
        expected_status,
        expected_message,
    )
