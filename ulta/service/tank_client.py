import multiprocessing
import os
import inspect
import logging
import yaml
from enum import IntEnum
from pathlib import Path
from typing import Tuple, Iterable, Protocol, Callable

from yandextank.common.interfaces import AbstractCriterion
from yandextank.common.util import Status
from yandextank.contrib.netort.netort.resource import ResourceManager
from yandextank.core.tankcore import LockError, Lock
from yandextank.core.tankworker import TankWorker
from yandextank.validator.validator import ValidationError

from ulta.common.exceptions import TankError
from ulta.common.job import Job, JobPluginType
from ulta.common.job_status import AdditionalJobStatus, JobStatus
from ulta.common.interfaces import JobDataUploaderClient
from ulta.service.data_uploader import TrailUploader, MonitoringUploader, DataPipePlugin
from ulta.service.imbalance_detector import ImbalanceUploader, ImbalanceDetectorPlugin
from ulta.service.interfaces import JobBackgroundWorker, JobFinalizer

INTERNAL_ERROR_TYPE = 'internal'
AUTOSTOP_EXIT_CODES = [
    value
    for attr, value in inspect.getmembers(AbstractCriterion, lambda a: not (inspect.isroutine(a)))
    if attr.startswith('RC')
]
TANK_WORKER_TIMEOUT = 60


class TankStatus(IntEnum):
    STATUS_UNSPECIFIED = 0
    READY_FOR_TEST = 1
    PREPARING_TEST = 2
    TESTING = 3
    TANK_FAILED = 4
    STOPPED = 5
    UPLOADING_ARTIFACTS = 6


class TankStatusProvider(Protocol):
    def get_tank_status(self) -> TankStatus:
        pass


class TankClient:
    _resource_manager_factory = None

    def __init__(
        self,
        logger: logging.Logger,
        tests_dir: str,
        lock_dir: str,
        loadtesting_client: JobDataUploaderClient,
        data_uploader_api_address: str,
        tank_worker_timeout: int = TANK_WORKER_TIMEOUT,
    ):
        self.logger = logger
        self.tests_dir = tests_dir
        self.lock_dir = lock_dir
        self.tank_worker = None
        self.loadtesting_client = loadtesting_client
        self.data_uploader_api_address = data_uploader_api_address
        self._tank_worker_start_shooting_event = None
        self._background_workers: list[JobBackgroundWorker] = []
        self._finalizers: list[JobFinalizer] = []
        self._tank_worker_timeout = tank_worker_timeout

    def _generate_job_config_patches(self, job: Job) -> list:
        patch = {
            'core': {
                'artifacts_base_dir': self.tests_dir,
                'lock_dir': self.lock_dir,
            },
        }
        if phantom := job.config.get('phantom', None):
            if 'cache_dir' not in phantom:
                patch.update(
                    {
                        'phantom': {'cache_dir': os.path.join(self.tests_dir, 'stpd-cache')},
                    }
                )
        patch.update(self._generate_disable_data_uploaders_patch(job))
        return [yaml.dump(patch)]

    def _generate_disable_data_uploaders_patch(self, job: Job) -> dict:
        patch = {}
        for key, plugin in job.get_plugins(JobPluginType.UPLOADER):
            if plugin.get('api_address') == self.data_uploader_api_address:
                patch[key] = {'enabled': False}
        return patch

    def dump_job_config(self, job: Job) -> str:
        tank_config_path = os.path.join(self.tests_dir, 'config')
        with open(tank_config_path, 'w') as f:
            yaml.dump(job.config, f)
        return tank_config_path

    def prepare_job(self, job: Job, files: Iterable[str]) -> Job:
        if self._is_test_session_running():
            raise TankError('Another test is already running')

        patches = self._generate_job_config_patches(job)
        tank_config_path = self.dump_job_config(job)
        try:
            self._tank_worker_start_shooting_event = multiprocessing.Event()
            self.tank_worker = TankWorker(
                [tank_config_path],
                None,
                patches,
                files=files,
                run_shooting_event=self._tank_worker_start_shooting_event,
                resource_manager=self._resource_manager_factory() if self._resource_manager_factory else None,
                plugins_implicit_enabling=True,
            )
            self.tank_worker.collect_files()
            self.tank_worker.go_to_test_folder()
        except (ValidationError, LockError) as e:
            raise TankError(str(e)) from e
        job.tank_job_id = self.tank_worker.test_id
        job.artifact_dir_path = self.get_dir_for_test(job.tank_job_id)

        self._register_workers(job)
        self.tank_worker.start()
        return job

    def _register_workers(self, job: Job):
        trail_pipe, mon_pipe = multiprocessing.Queue(), multiprocessing.Queue()
        self.tank_worker.core.register_external_plugin(
            'ulta_data_pipe', lambda core: DataPipePlugin(core, trail_pipe, mon_pipe)
        )
        self._add_background_worker(TrailUploader(job.id, trail_pipe, self.loadtesting_client, self.logger))
        self._add_background_worker(MonitoringUploader(job.id, mon_pipe, self.loadtesting_client, self.logger))
        if job.plugin_enabled(JobPluginType.AUTOSTOP):
            autostop_pipe = multiprocessing.Queue()
            self.tank_worker.core.register_external_plugin(
                'ulta_imbalance_detector', lambda core: ImbalanceDetectorPlugin(core, autostop_pipe)
            )
            self._add_finalizer(ImbalanceUploader(self.logger, job.id, autostop_pipe, self.loadtesting_client))

    def _add_background_worker(self, worker):
        self._background_workers.append(worker)

    def _add_finalizer(self, worker):
        self._finalizers.append(worker)

    def cleanup(self):
        [u.stop() for u in self._background_workers]
        self._background_workers = []
        self._finalizers = []
        if self.tank_worker is not None and self.tank_worker.is_alive():
            self.tank_worker.kill()
        self.tank_worker = None
        self._tank_worker_start_shooting_event = None

    def finish(self):
        self.stop_job()
        for worker in self._background_workers:
            worker.finish()
        for post_action in self._finalizers:
            post_action.run()
        self.cleanup()

    def run_job(self):
        if self._tank_worker_start_shooting_event is None:
            raise TankError('Trying to run job before prepare stage.')
        if not self._tank_worker_start_shooting_event.is_set():
            self._tank_worker_start_shooting_event.set()
            [u.start() for u in self._background_workers]

    def stop_job(self):
        if self.tank_worker is not None and self.tank_worker.is_alive():
            self.tank_worker.stop()
            self.tank_worker.join(self._tank_worker_timeout)

    def is_idle(self) -> bool:
        return self.get_tank_status() not in [TankStatus.PREPARING_TEST, TankStatus.TESTING]

    def _is_test_session_running(self) -> bool:
        if self.tank_worker and self.tank_worker.is_alive():
            return self.tank_worker.status != Status.TEST_FINISHED
        else:
            # Lock.is_locked returns `bool | str`
            return bool(Lock.is_locked(self.lock_dir))

    def _is_test_session_preparing(self):
        return self.tank_worker and self.tank_worker.is_alive() and self.tank_worker.status == Status.TEST_PREPARING

    def _is_active_test(self, test_id: str):
        return (
            self.tank_worker and self.tank_worker.test_id == test_id and self.tank_worker.status != Status.TEST_FINISHED
        )

    def get_tank_status(self) -> TankStatus:
        if self._is_test_session_preparing():
            return TankStatus.PREPARING_TEST
        elif self._is_test_session_running():
            return TankStatus.TESTING
        else:
            return TankStatus.READY_FOR_TEST

    def get_job_status(self, job_id: str) -> JobStatus:
        if self._is_active_test(job_id):
            return JobStatus.from_status(self.tank_worker.status)

        test_dir = self.get_dir_for_test(job_id)
        if not os.path.exists(test_dir):
            self.logger.warning('get job status: %s directory not found', test_dir)
            return JobStatus.from_status(Status.TEST_FINISHED)
        finish_status_file = os.path.join(test_dir, TankWorker.FINISH_FILENAME)
        if not os.path.exists(finish_status_file):
            self.logger.warning('get_job_status: %s file not found', finish_status_file)
            return JobStatus.from_status(Status.TEST_FINISHED)
        try:
            with open(finish_status_file) as f:
                return self.parse_job_status(yaml.safe_load(f) or {})
        except yaml.YAMLError:
            self.logger.exception("couldn't parse job status file")
            return JobStatus.from_status(
                AdditionalJobStatus.FAILED,
                "couldn't parse job status file",
                INTERNAL_ERROR_TYPE,
            )

    @staticmethod
    def extract_error(job_status_json: dict) -> Tuple[str, str]:
        error = job_status_json.get('error', '')
        error_type = None
        exit_code = job_status_json.get('exit_code')
        if not error:
            error = job_status_json.get('tank_msg', '')
            if error:
                error_type = INTERNAL_ERROR_TYPE
            elif exit_code and exit_code not in AUTOSTOP_EXIT_CODES:
                error = 'Unknown generator error'
        return error, error_type

    @staticmethod
    def parse_job_status(job_status_json: dict) -> JobStatus:
        error, error_type = TankClient.extract_error(job_status_json)
        exit_code = job_status_json.get('exit_code')
        if not error:
            if exit_code in AUTOSTOP_EXIT_CODES:
                job_status = AdditionalJobStatus.AUTOSTOPPED
            else:
                job_status = job_status_json.get('status_code', AdditionalJobStatus.FAILED)
        else:
            job_status = AdditionalJobStatus.FAILED
        return JobStatus.from_status(
            status=job_status,
            error=error,
            error_type=error_type,
            exit_code=exit_code,
        )

    @classmethod
    def use_resource_manager(cls, resource_manager_factory: Callable[[], ResourceManager]):
        cls._resource_manager_factory = resource_manager_factory

    def get_dir_for_test(self, tank_job_id: str) -> str:
        return str(Path(os.path.join(self.tests_dir, tank_job_id)).absolute())
