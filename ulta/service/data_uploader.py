import logging
import threading
from multiprocessing import Queue
import time
import typing

from ulta.common.interfaces import JobDataUploaderClient
from ulta.common.reporter import Reporter, ReporterHandlerProtocol
from yandextank.common.interfaces import AggregateResultListener, MonitoringDataListener, AbstractPlugin
from yandextank.core import TankCore
from yandextank.plugins.DataUploader.plugin import chop


class DataPipePlugin(AbstractPlugin, AggregateResultListener, MonitoringDataListener):
    def __init__(self, tank_core: TankCore, data_queue: Queue, monitoring_queue: Queue):
        AbstractPlugin.__init__(self, tank_core, {}, 'UltaDataPipe')
        self.data_queue = data_queue
        self.monitoring_queue = monitoring_queue
        self.core = tank_core
        self.chunk_size = 500000

    def prepare_test(self):
        self.core.job.subscribe_plugin(self)

    def post_process(self, rc):
        self.retcode = rc
        self.data_queue.put(None)
        self.data_queue.close()
        self.monitoring_queue.put(None)
        self.monitoring_queue.close()
        self.finished = True
        return rc

    def monitoring_data(self, data_list):
        if not self.interrupted.is_set():
            if len(data_list) > 0:
                [self.monitoring_queue.put(chunk) for chunk in chop(data_list, self.chunk_size)]

    def on_aggregated_data(self, data, stats):
        if not self.interrupted.is_set():
            self.data_queue.put((data, stats))


class TrailReportHandler(ReporterHandlerProtocol):
    name: str = 'Trail Uploader'

    def __init__(
        self,
        logger: logging.Logger,
        loadtesting_client: JobDataUploaderClient,
        job_id: str,
        error_handler: typing.Callable[[Exception, logging.Logger], None],
        max_batch_size: int | None = None,
    ):
        self._logger = logger
        self._lt_client = loadtesting_client
        self._job_id = job_id
        self._error_handler = error_handler
        self._max_batch_size = max_batch_size
        self.finished = threading.Event()

    def get_max_batch_size(self) -> int | None:
        return self._max_batch_size

    def handle(self, request_id: str, messages: list[tuple]):
        data = []
        for msg in messages:
            if msg is None:
                self.finished.set()
            else:
                data.extend(self._lt_client.prepare_test_data(msg[0], msg[1]))
        if data:
            self._lt_client.send_trails(self._job_id, data)

    def error_handler(self, error: Exception, logger: logging.Logger):
        if self._error_handler is not None:
            self._error_handler(error, logger)


class MonitoringReportHandler(ReporterHandlerProtocol):
    name: str = 'Monitoring Uploader'
    chunk_size: int = 10

    def __init__(
        self,
        logger: logging.Logger,
        loadtesting_client: JobDataUploaderClient,
        job_id: str,
        error_handler: typing.Callable[[Exception, logging.Logger], None],
        max_batch_size: int | None = None,
    ):
        self._logger = logger
        self._lt_client = loadtesting_client
        self._job_id = job_id
        self._error_handler = error_handler
        self._max_batch_size = max_batch_size
        self.finished = threading.Event()

    def get_max_batch_size(self) -> int | None:
        return self._max_batch_size

    def handle(self, request_id: str, messages: list):
        data = []
        for msg in messages:
            if msg is None:
                self.finished.set()
            else:
                data.extend(self._lt_client.prepare_monitoring_data(msg))
        if data:
            self._lt_client.send_monitorings(self._job_id, data)

    def error_handler(self, error: Exception, logger: logging.Logger):
        if self._error_handler is not None:
            self._error_handler(error, logger)


class DataUploaderThread:
    def __init__(
        self,
        logger: logging.Logger,
        trail_reporter: Reporter,
        monitoring_reporter: Reporter,
        handlers: list[TrailReportHandler | MonitoringReportHandler],
        shutdown_timeout: float,
    ):
        self._logger = logger
        self._trail_reporter = trail_reporter
        self._mon_reporter = monitoring_reporter
        self._handlers = handlers
        self._shutdown_timeout = shutdown_timeout
        self._evt_is_started = threading.Event()
        self._evt_need_stop = threading.Event()
        self._thr: threading.Thread | None = None

    def _run(self):
        try:
            self._logger.debug('DataUploaderThread: starting trail and monitoring uploaders')
            with (
                self._mon_reporter.run() as mon_stop,
                self._trail_reporter.run() as trail_stop,
            ):
                self._evt_is_started.set()
                self._logger.debug('DataUploaderThread: uploaders started')

                self._evt_need_stop.wait()
                self._logger.debug('DataUploaderThread: got stop event.')

                self._wait_for_data_sent()

                trail_stop.set()
                mon_stop.set()
            self._logger.debug('DataUploaderThread: done')
        except Exception:
            self._logger.exception('DataUploaderThread: unexpected exception')

    def _wait_for_data_sent(self):
        start_time = time.time()
        while time.time() - start_time < self._shutdown_timeout:
            for h in self._handlers:
                if not h.finished.is_set():
                    h.finished.wait(1)
                    break
            else:
                break
        elapsed_time = time.time() - start_time
        if elapsed_time >= self._shutdown_timeout:
            self._logger.warning(
                f'DataUploaderThread: wait for send data takes {elapsed_time} seconds (timeout is {self._shutdown_timeout}).'
            )
        elif elapsed_time > 1:
            self._logger.info(
                f'DataUploaderThread: wait for send data takes {elapsed_time} seconds (timeout is {self._shutdown_timeout}).'
            )

    def start(self):
        if self._thr is not None or self._evt_need_stop.is_set():
            raise RuntimeError('Ulta data uploaders already started')
        self._thr = threading.Thread(target=self._run, name='DataUploaderThread')
        self._thr.start()
        while not self._evt_is_started.is_set():
            self._evt_is_started.wait(10)
            if not self._thr.is_alive():
                raise RuntimeError('Error at start ulta data uploaders')

    def stop(self):
        self._evt_need_stop.set()
        if self._thr is None:
            return
        while self._thr.is_alive():
            self._thr.join(1)
