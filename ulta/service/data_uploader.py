import logging
import threading
import time
from multiprocessing import Queue
from queue import Empty

from ulta.common.interfaces import JobDataUploaderClient
from ulta.service.interfaces import JobBackgroundWorker
from ulta.common.exceptions import LOADTESTING_UNAVAILABLE_ERRORS
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


class DataUploader(JobBackgroundWorker):
    name: str = 'Abstract Data Uploader'

    class _Interrupt(Exception):
        pass

    def __init__(self, job_id: str, data_queue: Queue, logger: logging.Logger):
        self.job_id = job_id
        self.data_queue = data_queue
        self.finished = threading.Event()
        self.interrupted = threading.Event()
        self.thread = threading.Thread(target=self._uploader)
        self.thread.daemon = True
        self.logger = logger
        self.api_attempts = 10
        self.api_timeout = 5
        self.trace = False

    def start(self):
        if self.finished.is_set():
            raise RuntimeError(f"Worker {self.name} can't be started after finish was called.")
        self.thread.start()

    def stop(self):
        self.interrupted.set()

    def finish(self):
        if self.finished.is_set():
            return
        # give an attempt to finish gracefully: finishing uploading all the stuff
        if self.thread.is_alive():
            self.thread.join(self.api_attempts * self.api_timeout)

        # final attempt to finish
        if self.thread.is_alive():
            self.stop()
            self.thread.join(self.api_timeout)
            self._purge_data_queue()
        self.finished.set()

    def api_timeouts(self):
        return (self.api_timeout for _ in range(self.api_attempts - 1))

    def _uploader(self):
        self.logger.info('%(worker_name)s thread started', dict(worker_name=self.name))
        try:
            while not self.interrupted.is_set():
                try:
                    entry = self.data_queue.get(timeout=1)
                    if entry is None:
                        self.logger.info(
                            '%(worker_name)s queue returned None. No more messages expected.',
                            dict(worker_name=self.name),
                        )
                        break
                    self._send_with_retry(entry)
                except Empty:
                    continue
                except DataUploader._Interrupt:
                    self.logger.error(
                        '%(worker_name)s uploader failed to connect to backend. Terminating...',
                        dict(worker_name=self.name),
                    )
                    self.interrupted.set()
                except Exception as e:
                    self.logger.exception(
                        'Unhandled exception occured. Skipping data chunk...', dict(error=str(e), worker_name=self.name)
                    )

            if self.interrupted.is_set():
                self.logger.warning('%(worker_name)s received interrupt signal', dict(worker_name=self.name))

            self._purge_data_queue()
        finally:
            self.logger.info('Closing %(worker_name)s thread', dict(worker_name=self.name))
            self.finished.set()

    def _purge_data_queue(self):
        try:
            while self.data_queue is not None and not self.data_queue.empty():
                if self.data_queue.get_nowait() is None:
                    break
        except Empty:
            pass

    def _send_with_retry(self, data):
        data = self.prepare_data(data)
        api_timeouts = self.api_timeouts()
        while not self.interrupted.is_set():
            try:
                if self.trace:
                    self.logger.debug('Sending %s', data)
                code = self.send_data(data)
                if code == 0:
                    break
            except LOADTESTING_UNAVAILABLE_ERRORS:
                if not self.interrupted.is_set():
                    try:
                        timeout = next(api_timeouts)
                    except StopIteration:
                        raise DataUploader._Interrupt()
                    self.logger.info(
                        'GRPC error, will retry in %(next_attempt)ss...',
                        dict(next_attempt=timeout, worker_name=self.name),
                    )
                    time.sleep(timeout)
                    continue
                else:
                    break

    def send_data(self, data):
        raise NotImplementedError()

    def prepare_data(self, data):
        return data


class TrailUploader(DataUploader):
    name: str = 'Trail Uploader'

    def __init__(
        self, job_id: str, data_queue: Queue, loadtesting_client: JobDataUploaderClient, logger: logging.Logger
    ):
        DataUploader.__init__(self, job_id, data_queue, logger)
        self.loadtesting_client = loadtesting_client

    def prepare_data(self, data):
        data_item, stat_item = data
        return self.loadtesting_client.prepare_test_data(data_item, stat_item)

    def send_data(self, data):
        if not self.interrupted.is_set():
            return self.loadtesting_client.send_trails(self.job_id, data)
        return 0


class MonitoringUploader(DataUploader):
    name: str = 'Monitoring Uploader'
    chunk_size: int = 10

    def __init__(
        self, job_id: str, data_queue: Queue, loadtesting_client: JobDataUploaderClient, logger: logging.Logger
    ):
        DataUploader.__init__(self, job_id, data_queue, logger)
        self.loadtesting_client = loadtesting_client

    def send_data(self, data):
        if not self.interrupted.is_set():
            return self.loadtesting_client.send_monitorings(self.job_id, data)
        return 0

    def prepare_data(self, data_item):
        return self.loadtesting_client.prepare_monitoring_data(data_item)
