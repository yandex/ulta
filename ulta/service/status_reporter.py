import logging
import threading
from contextlib import contextmanager
from google.api_core.exceptions import (
    FailedPrecondition,
    NotFound,
    Unauthorized,
    Unauthenticated,
)
from typing import Optional
from ulta.common.cancellation import Cancellation, CancellationType
from ulta.common.interfaces import TankStatusClient
from ulta.service.tank_client import TankStatusProvider, TankStatus


class StatusReporter:
    def __init__(
        self,
        logger: logging.Logger,
        tank_client: TankStatusProvider,
        loadtesting_client: TankStatusClient,
        cancellation: Cancellation,
        report_delay: int = 1,
    ) -> None:
        self.logger = logger
        self.tank_client = tank_client
        self.loadtesting_client = loadtesting_client
        self.report_delay = max(1, report_delay)
        self.cancellation = cancellation

    def report_tank_status(self, status: Optional[TankStatus] = None):
        status = status or self.tank_client.get_tank_status()
        self.loadtesting_client.claim_tank_status(status.name)

    @contextmanager
    def run(self):
        self._run()
        try:
            yield
        finally:
            self._stop()
            try:
                self.report_tank_status(TankStatus.STOPPED)
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
                    self.cancellation.notify(
                        '''
Backend denied this agent.
It is possible that agent was deleted from backend, or new agent with same name registered.
If this error keeps repeating - try to delete agentid file, or run agent with
`ulta --no-cache` or `LOADTESTING_NO_CACHE=1 ulta`
''',
                        CancellationType.FORCED,
                    )
                except Exception:
                    self.logger.exception('Failed to report agent status')
                finally:
                    stop.wait(self.report_delay)

        self._thread = threading.Thread(target=worker)
        self._thread.start()

    def _stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join()


class DummyStatusReporter:
    @contextmanager
    def run(self):
        yield
