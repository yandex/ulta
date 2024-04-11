import logging
import threading
from contextlib import contextmanager
from google.api_core.exceptions import (
    FailedPrecondition,
    NotFound,
    Unauthorized,
    Unauthenticated,
)
from ulta.common.cancellation import Cancellation, CancellationType
from ulta.common.interfaces import TankStatusClient
from ulta.common.state import State as ServiceState
from ulta.service.tank_client import TankStatusProvider, TankStatus, IDLE_STATUSES


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
        self._stop_event = None
        self._thread = None

    def report_tank_status(self, status: TankStatus | None = None, status_message: str | None = None):
        status = status or self.tank_client.get_tank_status()
        if status in IDLE_STATUSES and not self.service_state.ok:
            status = TankStatus.ERROR
            status_message = self.service_state.get_summary_message()

        self.loadtesting_client.claim_tank_status(status.name, status_message)

    @contextmanager
    def run(self):
        try:
            self._run()
            yield
        finally:
            self._stop()
            try:
                self.report_tank_status(TankStatus.STOPPED, self.cancellation.explain())
            except BaseException:
                self.logger.exception('Failed to report STOPPED status')

    def _run(self):
        stop = threading.Event()
        self._stop_event = stop

        def worker():
            while not stop.is_set():
                try:
                    self.report_tank_status()
                except (FailedPrecondition, NotFound, Unauthorized, Unauthenticated):
                    self.logger.exception("Backend doesn't recognize this agent. Performing shutdown.")
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
                except Exception:
                    self.logger.exception('Failed to report agent status')
                finally:
                    stop.wait(self.report_interval)

        self._thread = threading.Thread(target=worker)
        self._thread.start()

    def _stop(self):
        if self._stop_event:
            self._stop_event.set()
        if self._thread:
            self._thread.join()


class DummyStatusReporter:
    @contextmanager
    def run(self):
        yield
