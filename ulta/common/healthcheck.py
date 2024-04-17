import logging
import typing
from ulta.common.background_worker import run_background_worker
from ulta.common.state import GenericObserver


class HealthCheckProtocol(typing.Protocol):
    def healthcheck(self):
        ...


class HealthCheck:
    def __init__(
        self, observer: GenericObserver, healthchecks: list[HealthCheckProtocol], logger: logging.Logger | None = None
    ):
        self._healthchecks = healthchecks
        self._logger = logger or logging.getLogger('ulta_healthcheck')
        self._observer = observer
        self._worker = None

    def healthcheck(self):
        for hc in self._healthchecks:
            with self._observer.observe(stage='healthcheck'):
                hc.healthcheck()

    def run_healthcheck(self, healthcheck_interval=30):
        if self._worker is not None:
            raise Exception('HealthCheck is already running')

        def hc_failed(e: Exception):
            try:
                self._logger.exception('Healthcheck failed', exc_info=e)
            except BaseException:
                pass

        self.healthcheck()
        return run_background_worker(self.healthcheck, hc_failed, healthcheck_interval)
