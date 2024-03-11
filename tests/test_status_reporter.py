import pytest
from unittest.mock import MagicMock
from ulta.service.status_reporter import StatusReporter
from ulta.service.tank_client import TankStatus
from ulta.yc.ycloud import JWTError
from google.api_core.exceptions import FailedPrecondition, NotFound, Unauthenticated, Unauthorized


def test_report_tank_status():
    tank_client = MagicMock()
    tank_client.get_tank_status.return_value = TankStatus.TANK_FAILED
    loadtesting_client = MagicMock()
    reporter = StatusReporter(MagicMock(), tank_client, loadtesting_client, MagicMock())
    reporter.report_tank_status()
    loadtesting_client.claim_tank_status.assert_called_with(TankStatus.TANK_FAILED.name)


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
    reporter = StatusReporter(MagicMock(), MagicMock(), loadtesting_client, MagicMock())
    with pytest.raises(expected_exception):
        reporter.report_tank_status(TankStatus.TANK_FAILED)
    with reporter.run():
        reporter._stop_event.wait(3)
    assert reporter._stop_event.is_set()
    loadtesting_client.claim_tank_status.assert_called_with(TankStatus.STOPPED.name)
