import logging
from dataclasses import dataclass
from multiprocessing import Queue
from queue import Empty

from ulta.common.interfaces import JobDataUploaderClient
from ulta.service.interfaces import JobFinalizer
from yandextank.common.interfaces import AbstractPlugin
from yandextank.core import TankCore
from yandextank.plugins.Autostop.plugin import Plugin as TankAutostopPlugin


@dataclass
class ImbalanceEntry:
    timestamp: int
    rps: int
    message: str


class ImbalanceDetectorPlugin(AbstractPlugin):
    def __init__(self, tank_core: TankCore, data_queue: Queue):
        AbstractPlugin.__init__(self, tank_core, {}, 'UltaImbalanceDetector')
        self.core = tank_core
        self.data_queue = data_queue

    def post_process(self, retcode):
        self.retcore = retcode
        try:
            autostops: list[TankAutostopPlugin] = self.core.get_plugins_of_type(TankAutostopPlugin)
        except KeyError:
            self.log.debug('No autostop plugins found')
            self.data_queue.put(None)
            return retcode

        for autostop in autostops:
            if autostop and autostop.cause_criterion and autostop.imbalance_timestamp:
                self.data_queue.put(
                    ImbalanceEntry(
                        timestamp=autostop.imbalance_timestamp,
                        rps=autostop.imbalance_rps,
                        message=autostop.cause_criterion.explain(),
                    )
                )

        self.data_queue.put(None)
        return retcode


class ImbalanceUploader(JobFinalizer):
    name: str = 'Imbalance Uploader'

    def __init__(
        self, logger: logging.Logger, job_id: str, data_queue: Queue, loadtesting_client: JobDataUploaderClient
    ):
        self.logger = logger
        self.job_id = job_id
        self.data_queue = data_queue
        self.loadtesting_client = loadtesting_client

    def run(self) -> int:
        autostop: ImbalanceEntry | None = None
        try:
            # purge queue trying to find earliest autostop
            while True:
                data = self.data_queue.get_nowait()
                if isinstance(data, ImbalanceEntry) and (autostop is None or autostop.timestamp < data.timestamp):
                    autostop = data
        except Empty:
            pass

        if autostop is not None:
            self.logger.info('Set imbalance %s at %s. Comment: %s', autostop.rps, autostop.timestamp, autostop.message)
            self.loadtesting_client.set_imbalance_and_dsc(
                self.job_id, autostop.rps, autostop.timestamp, autostop.message
            )
        return 0
