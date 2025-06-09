import logging
import pytest
import time
import typing
from dataclasses import dataclass
from datetime import timedelta
from queue import Queue
from threading import Event
from unittest.mock import MagicMock
from ulta.common.cancellation import Cancellation
from ulta.common.exceptions import CompositeException
from ulta.common.reporter import Reporter, _chop, ReporterHandlerProtocol, _UnsentMessage
from ulta.common.state import State
from ulta.service.status_reporter import StatusReporter
from ulta.service.tank_client import TankStatus
from ulta.yc.ycloud import JWTError
from google.api_core.exceptions import FailedPrecondition, NotFound, Unauthenticated, Unauthorized


@dataclass
class ReporterHandler(ReporterHandlerProtocol):
    handle: typing.Callable[[str, typing.Any], None]
    error_handler: typing.Callable[[Exception, logging.Logger], None]
    max_batch_size: int | None = None

    def get_max_batch_size(self):
        return self.max_batch_size


@pytest.mark.parametrize(
    'data1, data2, max_batch_size, expected_result',
    [
        ([1, 2, 3], [15, 18], 100, [[1, 2, 3, 15, 18]]),
        ([1, 2, 3], [15, 18], 2, [[1, 2], [3, 15], [18]]),
    ],
)
def test_generic_reporter(data1, data2, max_batch_size, expected_result):
    q1, q2 = Queue(), Queue()
    for d in data1:
        q1.put_nowait(d)
    for d in data2:
        q2.put_nowait(d)

    logger = logging.getLogger()
    processed_messages = []

    def handler(_, msg):
        processed_messages.append(msg)

    def error_handler(error, logger):
        logger.exception('error is not expected', exc_info=error)
        raise Exception(f'error is not expected. got {error}')

    reporter = Reporter(
        q1,
        q2,
        logger=logger,
        handlers=ReporterHandler(handle=handler, error_handler=error_handler, max_batch_size=max_batch_size),
    )
    reporter.report()
    assert processed_messages == expected_result


def test_generic_reporter_retry_unsent_data():
    q1, q2 = Queue(), Queue()
    for d in range(5):
        q1.put_nowait(d)
    for d in range(10, 15):
        q2.put_nowait(d)

    logger = logging.getLogger()
    processed_messages = []
    handled_errors = []

    e1 = Exception(123)
    e2 = RuntimeError('hh')
    ticks = [None, None, e1, e1, None, e2]

    def handler(_, msg):
        if len(ticks) > 0:
            tick = ticks.pop(0)
            if tick is not None:
                raise tick

        processed_messages.append(msg)

    def error_handler(error, logger):
        handled_errors.append(error)
        if error in (e1, e2) or isinstance(error, CompositeException):
            return

        logger.exception('error is not expected', exc_info=error)
        raise Exception(f'error is not expected. got {error}')

    reporter = Reporter(
        q1, q2, logger=logger, handlers=ReporterHandler(handle=handler, error_handler=error_handler, max_batch_size=2)
    )
    reporter.report()

    assert processed_messages == [[0, 1], [2, 3], [13, 14]]
    assert len(handled_errors) == 1
    assert isinstance(handled_errors[0], CompositeException)
    assert handled_errors[0].errors == [e1, e1]

    reporter.report()

    assert processed_messages == [[0, 1], [2, 3], [13, 14], [11, 12]]
    assert len(handled_errors) == 2
    assert isinstance(handled_errors[1], RuntimeError)
    assert handled_errors[1].args == ('hh',)

    reporter.report()

    assert processed_messages == [[0, 1], [2, 3], [13, 14], [11, 12], [4, 10]]
    assert len(handled_errors) == 2


def test_generic_reporter_retention():
    q1 = Queue()
    steps_count = 5
    for d in range(steps_count):
        q1.put_nowait(d)

    logger = logging.getLogger()
    processed_messages = []
    handled_errors = []

    class Nop(Exception): ...

    def handler(_, msg):
        processed_messages.append(msg)
        raise Nop()

    def error_handler(error, logger):
        handled_errors.append(error)
        if isinstance(error, (Nop, CompositeException)):
            return

        logger.exception('error is not expected', exc_info=error)
        raise Exception(f'error is not expected. got {error}')

    reporter_handler = ReporterHandler(handle=handler, error_handler=error_handler, max_batch_size=None)
    reporter = Reporter(
        q1,
        logger=logger,
        handlers=reporter_handler,
        retention_period=timedelta(milliseconds=100),
    )

    reporter.report()

    assert len(handled_errors) == 1
    assert isinstance(handled_errors[0], CompositeException)
    assert all(isinstance(e, Nop) for e in handled_errors[0].errors)

    assert processed_messages == [[0], [1], [2], [3], [4]]
    assert id(reporter_handler) in reporter._unsent_messages
    assert len(reporter._unsent_messages[id(reporter_handler)]) == 5

    processed_messages.clear()

    reporter.report()

    assert len(handled_errors) == 2
    assert isinstance(handled_errors[1], CompositeException)
    assert all(isinstance(e, Nop) for e in handled_errors[1].errors)
    assert processed_messages == [[0], [1], [2], [3], [4]]
    assert len(reporter._unsent_messages[id(reporter_handler)]) == 5

    time.sleep(0.2)

    processed_messages.clear()
    reporter.report()

    assert processed_messages == []
    assert len(reporter._unsent_messages[id(reporter_handler)]) == 0


def test_generic_reporter_limits_unsent_queue():
    q1 = Queue()
    for d in range(30):
        q1.put_nowait(d)

    logger = logging.getLogger()
    processed_messages = []
    handled_errors = []

    e1 = Exception(123)
    e2 = RuntimeError('hh')
    ticks = [e1, e1, e1, e1, e1, e2, None]

    def handler(_, msg):
        if len(ticks) > 0:
            tick = ticks.pop(0)
            if tick is not None:
                raise tick

        processed_messages.append(msg)

    def error_handler(error, logger):
        handled_errors.append(error)
        if error in (e1, e2) or isinstance(error, CompositeException):
            return

        logger.exception('error is not expected', exc_info=error)
        raise Exception(f'error is not expected. got {error}')

    reporter_handler = ReporterHandler(handle=handler, error_handler=error_handler, max_batch_size=5)
    reporter = Reporter(
        q1,
        logger=logger,
        handlers=reporter_handler,
        max_unsent_size=15,
    )
    reporter.report()

    unsent_queue = reporter._unsent_messages[id(reporter_handler)]

    assert processed_messages == []
    assert len(unsent_queue) == 3
    assert unsent_queue[0].data == [15, 16, 17, 18, 19]
    assert unsent_queue[1].data == [20, 21, 22, 23, 24]
    assert unsent_queue[2].data == [25, 26, 27, 28, 29]

    reporter.report()

    assert processed_messages == [[15, 16, 17, 18, 19], [20, 21, 22, 23, 24], [25, 26, 27, 28, 29]]
    assert len(unsent_queue) == 0


def test_generic_reporter_put_unsent():
    reporter = Reporter(
        Queue(),
        logger=logging.getLogger(),
        handlers=[],
        max_unsent_size=15,
    )
    handler = object()
    reporter._put_unsent(handler, _UnsentMessage([1, 2, 3, 4, 5]))
    reporter._put_unsent(handler, _UnsentMessage([6, 7, 8]))
    reporter._put_unsent(handler, _UnsentMessage([9, 10, 11]))
    reporter._put_unsent(handler, _UnsentMessage([12, 13, 14]))

    unsent_queue = reporter._unsent_messages[id(handler)]

    assert len(unsent_queue) == 4
    assert unsent_queue[0].data == [1, 2, 3, 4, 5]
    assert unsent_queue[1].data == [6, 7, 8]
    assert unsent_queue[2].data == [9, 10, 11]
    assert unsent_queue[3].data == [12, 13, 14]

    reporter._put_unsent(handler, _UnsentMessage([15, 16]))

    assert len(unsent_queue) == 4
    assert unsent_queue[0].data == [6, 7, 8]
    assert unsent_queue[1].data == [9, 10, 11]
    assert unsent_queue[2].data == [12, 13, 14]
    assert unsent_queue[3].data == [15, 16]

    reporter._put_unsent(handler, _UnsentMessage([17, 18, 19]))

    assert len(unsent_queue) == 5
    assert unsent_queue[0].data == [6, 7, 8]
    assert unsent_queue[1].data == [9, 10, 11]
    assert unsent_queue[2].data == [12, 13, 14]
    assert unsent_queue[3].data == [15, 16]
    assert unsent_queue[4].data == [17, 18, 19]

    reporter._put_unsent(handler, _UnsentMessage([20, 21, 22, 23, 24, 25, 26, 27, 28]))

    assert len(unsent_queue) == 3
    assert unsent_queue[0].data == [15, 16]
    assert unsent_queue[1].data == [17, 18, 19]
    assert unsent_queue[2].data == [20, 21, 22, 23, 24, 25, 26, 27, 28]


def test_generic_reporter_run():
    q1, q2 = Queue(), Queue()
    data1 = [1, 2, 3]
    data2 = [15, 18]
    before_finish = [0, 0, 109]

    def nexter(q: Queue, data: list):
        it = iter(data)

        def _handler():
            try:
                q.put_nowait(next(it))
                return True
            except StopIteration:
                return False

        return _handler

    class Stop(Exception): ...

    next1 = nexter(q1, data1)
    next2 = nexter(q2, data2)

    logger = logging.getLogger()
    processed_messages = []

    next1()
    next2()

    assert not q1.empty()
    assert not q2.empty()

    def handler(_, msg):
        processed_messages.append(msg)
        n1, n2 = next1(), next2()
        if not n1 and not n2:
            raise Stop()

    test_finish = Event()

    def error_handler(error, logger):
        if not isinstance(error, Stop):
            logger.exception('error is not expected', exc_info=error)
            raise Exception(f'error is not expected. got {error}')

        test_finish.set()

    reporter = Reporter(
        q1,
        q2,
        logger=logger,
        handlers=ReporterHandler(handle=handler, error_handler=error_handler, max_batch_size=10),
        report_interval=0.1,
    )
    with reporter.run():
        test_finish.wait(1)
        assert processed_messages == [[1, 15], [2, 18], [3]]
        for d in before_finish:
            q1.put_nowait(d)

    # extra [3] because it issued the exception and must be retried
    assert processed_messages == [[1, 15], [2, 18], [3], [3], [0, 0, 109]]


@pytest.mark.parametrize(
    'data, size, expected',
    [
        ([1, 2, 3, 4, 5, 6, 7, 8, 9, 0], 10, [[1, 2, 3, 4, 5, 6, 7, 8, 9, 0]]),
        ([], 10, []),
        ([1, 2, 3, 4, 5, 6, 7, 8, 9, 0], 0, [[1, 2, 3, 4, 5, 6, 7, 8, 9, 0]]),
        ([1, 2, 3, 4, 5, 6, 7, 8, 9, 0], 2, [[1, 2], [3, 4], [5, 6], [7, 8], [9, 0]]),
        ([1, 2, 3, 4, 5, 6, 7, 8, 9, 0], 9, [[1, 2, 3, 4, 5, 6, 7, 8, 9], [0]]),
        ([1, 2, 3, 4, 5, 6, 7, 8, 9, 0], 3, [[1, 2, 3], [4, 5, 6], [7, 8, 9], [0]]),
    ],
)
def test_generic_reporter_chop(data, size, expected):
    assert expected == _chop(data, size)


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
