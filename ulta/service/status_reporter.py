import logging
from contextlib import contextmanager
from google.api_core.exceptions import (
    FailedPrecondition,
    NotFound,
    Unauthorized,
    Unauthenticated,
)
from ulta.common.background_worker import run_background_worker
from ulta.common.cancellation import Cancellation, CancellationType
from ulta.common.interfaces import TankStatusClient
from ulta.common.state import State as ServiceState
from ulta.common.utils import truncate_string
from ulta.service.tank_client import TankStatusProvider, TankStatus, IDLE_STATUSES

CLAIM_STATUS_MESSAGE_LIMIT = 8000


class StatusReporter:
    def __init__(
        self,
        logger: logging.Logger,
        tank_client: TankStatusProvider,
        loadtesting_client: TankStatusClient,
        cancellation: Cancellation,
        service_state: ServiceState,
        report_interval: float = 1,
    ) -> None:
        self.logger = logger
        self.tank_client = tank_client
        self.loadtesting_client = loadtesting_client
        self.report_interval = max(1, report_interval)
        self.cancellation = cancellation
        self.service_state = service_state
        self._thread = None

    def report_tank_status(self, status: TankStatus | None = None, status_message: str | None = None):
        status = status or self.tank_client.get_tank_status()
        if status in IDLE_STATUSES and not self.service_state.ok:
            status = TankStatus.ERROR
            status_message = self.service_state.get_summary_message()

        self.loadtesting_client.claim_tank_status(
            status.name, truncate_string(status_message, CLAIM_STATUS_MESSAGE_LIMIT, cut_in_middle=True)
        )

    @contextmanager
    def run(self):
        try:
            with run_background_worker(self.report_tank_status, self._handle_error, self.report_interval) as cancel:
                yield cancel
        finally:
            try:
                self.report_tank_status(TankStatus.STOPPED, self.cancellation.explain())
            except BaseException:
                self.logger.exception('Failed to report STOPPED status')

    def _handle_error(self, e: Exception):
        try:
            if isinstance(e, (FailedPrecondition, NotFound, Unauthorized, Unauthenticated)):
                self.logger.error("Backend doesn't recognize this agent. Performing shutdown.", exc_info=e)
                self.logger.error(
                    '''
    Backend denied this agent.
    It is possible that agent was deleted from backend, or new agent with same name registered.
    If this error keeps repeating - try to delete agentid file, or run agent with
    `ulta --no-cache` or `LOADTESTING_NO_CACHE=1 ulta`
    '''
                )
                self.cancellation.notify(
                    "The backend doesn't know this agent: agent has been deleted or account is missing loadtesting.generatorClient role.",
                    CancellationType.FORCED,
                )
            else:
                self.logger.error('Failed to report agent status', exc_info=e)
        except BaseException:
            pass


class DummyStatusReporter:
    @contextmanager
    def run(self):
        yield
