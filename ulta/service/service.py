import json
import logging
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable

from google.api_core.exceptions import (
    ClientError,
    FailedPrecondition,
    InternalServerError,
    NotFound,
)
from ulta.common.exceptions import (
    ObjectStorageError,
    TankError,
    InvalidJobDataError,
    LOADTESTING_UNAVAILABLE_ERRORS,
    JobNotExecutedError,
    JobStoppedError,
)
from ulta.common.ammo import Ammo
from ulta.common.cancellation import Cancellation, CancellationRequest
from ulta.common.file_system import ensure_dir
from ulta.common.interfaces import LoadtestingClient, S3Client, NamedService
from ulta.common.job_status import AdditionalJobStatus, JobStatus, FINISHED_STATUSES_TO_EXIT_CODE_MAPPING
from ulta.common.job import Job, JobResult, ArtifactSettings
from ulta.common.logging import get_logger, get_event_logger
from ulta.common.state import State, GenericObserver
from ulta.service.artifact_uploader import ArtifactUploader
from ulta.service.tank_client import TankClient, TankStatus, INTERNAL_ERROR_TYPE

LOCK_DIR = '/var/lock'
FINISHED_FILE = 'finish_status.yaml'


class UltaService:
    def __init__(
        self,
        state: State,
        loadtesting_client: LoadtestingClient,
        tank_client: TankClient,
        s3_client: S3Client,
        tmp_dir: Path,
        sleep_time: float,
        artifact_uploaders: Iterable[NamedService[ArtifactUploader]],
        cancellation: Cancellation,
        max_waiting_time: int = 300,
        logger: logging.Logger | None = None,
        event_logger: logging.Logger | None = None,
    ):
        self.logger = logger or get_logger()
        self.event_logger = event_logger or get_event_logger()

        self.cancellation = cancellation
        self.tmp_dir = tmp_dir
        self.loadtesting_client = loadtesting_client
        self.s3_client = s3_client
        self.tank_client = tank_client
        self.sleep_time = sleep_time
        self.job_pooling_delay = sleep_time
        self.artifact_uploaders = artifact_uploaders
        self.max_waiting_time = max_waiting_time
        self._override_status: TankStatus | None = None
        self._observer = GenericObserver(state, self.logger, cancellation)

    def get_tank_status(self) -> TankStatus:
        if self._override_status is not None:
            return self._override_status
        return self.tank_client.get_tank_status()

    def _extract_ammo(self, job_message, test_data_dir) -> list[Ammo]:
        res = []
        for payload_entry in job_message.data_payload:
            ammo_name = payload_entry.name
            if not ammo_name:
                self.logger.warning('Test data specified with no name.', dict(test_id=job_message.id))
                raise InvalidJobDataError('Test data specified with no name.')

            ammo_file_path = os.path.join(test_data_dir, ammo_name.strip('/'))
            ammo_file_path = os.path.normpath(ammo_file_path)
            if os.path.commonpath((test_data_dir, ammo_file_path)) != os.path.normpath(test_data_dir):
                self.logger.error('Can\'t write ammo file to %(dest_file_name)s', dict(dest_file_name=ammo_file_path))
                raise InvalidJobDataError('Invalid test data name')

            if payload_entry.is_transient:
                self.logger.info(
                    'Downloading transient ammo %(ammo_name)s',
                    dict(test_id=job_message.id, ammo_name=ammo_name, dest_file_name=ammo_file_path),
                )
                self.loadtesting_client.download_transient_ammo(
                    job_id=job_message.id,
                    ammo_name=ammo_name,
                    path_to_download=ammo_file_path,
                )
            else:
                self.logger.info(
                    'Downloading s3 file from %(bucket)s/%(file_name)s',
                    dict(
                        bucket=payload_entry.storage_object.object_storage_bucket,
                        file_name=payload_entry.storage_object.object_storage_filename,
                        dest_file_name=ammo_file_path,
                    ),
                )
                self.s3_client.download(
                    storage_object=payload_entry.storage_object,
                    path_to_download=ammo_file_path,
                )

            res.append(Ammo(ammo_name, ammo_file_path))

        return res

    def claim_post_job_error(self, job: Job, error, error_type=None):
        try:
            self.loadtesting_client.claim_job_status(
                job.id, AdditionalJobStatus.JOB_STATUS_UNSPECIFIED, error, error_type
            )
        except Exception as e:
            self.logger.exception(
                'Failed to update test %(test_id)s status to %(error_type)s: %(error)s',
                dict(test_id=job.id, error_type=error_type, error=str(e)),
            )

    def claim_job_status(self, job: Job, status: JobStatus):
        job.update_status(status)
        if status.status in FINISHED_STATUSES_TO_EXIT_CODE_MAPPING:
            self._report_job_event(job, status)
        self.loadtesting_client.claim_job_status(job.id, status.status, status.error, status.error_type)

    def claim_job_failed(self, job: Job, error, error_type=None):
        return self.claim_job_status(
            job,
            JobStatus.from_status(
                status=AdditionalJobStatus.FAILED,
                error=error,
                error_type=error_type,
            ),
        )

    def _report_job_event(self, job: Job, status: JobStatus):
        msg = ['Test %(test_id)s execution completed with status %(status)s']
        labels = dict(test_id=job.id, internal_id=job.tank_job_id, status=status.status)
        report_func = self.event_logger.info
        if status.error:
            msg.append('error: %(error)s')
            labels['error'] = status.error
            report_func = self.event_logger.error
        report_func(', '.join(msg), labels)

    def get_job(self, job_id: str | None = None) -> Job | None:
        try:
            job_message = self.loadtesting_client.get_job(job_id)
        except NotFound:
            self.logger.debug('No pending jobs for agent')
            return None

        if job_message is None or not job_message.id:
            return None

        job = Job(id=job_message.id)
        self.event_logger.info('Got new test %(test_id)s for execution', dict(test_id=job.id))
        try:
            test_data_dir = ensure_dir(self.tmp_dir / f'test_data_{job_message.id}')
            job.log_group_id = job_message.logging_log_group_id
            job.config = json.loads(job_message.config)
            job.test_data_dir = test_data_dir.absolute().as_posix()
            job.upload_artifact_settings = self.extract_artifact_settings(job_message)
            job.ammos = self._extract_ammo(job_message, job.test_data_dir)
            return job
        except json.JSONDecodeError as error:
            self.logger.exception('Invalid test config format', dict(test_id=job_message.id, error=str(error)))
            self.claim_job_failed(job, f'Invalid test config:{str(error)}', 'JOB_CONFIG')
        except (
            ObjectStorageError,
            ClientError,
            InvalidJobDataError,
        ) as error:
            self.logger.exception('Error loading test data', dict(test_id=job_message.id, error=str(error)))
            self.claim_job_failed(job, f'Error loading test data: {str(error)})', 'JOB_AMMO')
        except Exception as error:
            self.logger.exception('Unknown exception', dict(test_id=job_message.id, error=str(error)))
            self.claim_job_failed(job, f'Unknown error occured: {str(error)})', 'UNKNOWN')
            raise
        return None

    @staticmethod
    def extract_artifact_settings(job_message):
        if job_message.HasField('artifact_upload_settings'):
            msg = job_message.artifact_upload_settings
            if msg.output_bucket and msg.output_name:
                return ArtifactSettings(
                    output_bucket=msg.output_bucket,
                    output_name=msg.output_name,
                    is_archive=msg.is_archive,
                    filter_include=msg.filter_include,
                    filter_exclude=msg.filter_exclude,
                )
        return None

    def wait_for_a_job(self) -> Job:
        while True:
            self.cancellation.raise_on_set()
            with self._observer.observe(stage='request new test from backend', critical=False, suppress=False):
                if job := self.get_job():
                    return job
            time.sleep(self.job_pooling_delay)

    def await_tank_is_ready(self, timeout=60):
        if not self.tank_client.is_idle():
            self.logger.warning('There is active testing session. Awaiting for finish')
            stop_waiting_at = time.time() + (timeout or self.max_waiting_time)
            while not self.tank_client.is_idle():
                self.cancellation.raise_on_set()
                if time.time() >= stop_waiting_at:
                    raise TankError('Tank is busy for too long. Cancelling job.')
                time.sleep(self.sleep_time)

    @staticmethod
    def _get_job_data_paths(path: str) -> list[str]:
        if not path or not os.path.isdir(path):
            return []
        return [os.path.join(path, x) for x in os.listdir(path)]

    def serve_lt_job(self, job: Job):
        done = False
        while not done:
            self.cancellation.raise_on_set()
            with self.sustain_job():
                self.serve_lt_signal(job.id)
                job_status = self.tank_client.get_job_status(job.tank_job_id)
                if done := job_status.finished():
                    self.tank_client.finish()
                self.claim_job_status(job, job_status)
            time.sleep(self.sleep_time)

    def serve_lt_signal(self, job_id: str):
        signal = self.loadtesting_client.get_job_signal(job_id)
        name = signal.Signal.Name(signal.signal)
        if name == 'STOP':
            self.serve_stop_signal()
        elif name == 'RUN_IN':
            self.serve_run_signal(signal.run_in)
        elif name in ['WAIT', 'SIGNAL_UNSPECIFIED']:
            pass
        else:
            raise ValueError(f'Unknown signal {name} returned from server')

    def serve_stop_signal(self) -> None:
        self.tank_client.stop_job()
        raise JobStoppedError()

    def serve_run_signal(self, run_in) -> None:
        if run_in > self.sleep_time:
            return
        if run_in > 0:
            time.sleep(run_in)
        self.tank_client.run_job()

    def serve(self):
        try:
            while not self.cancellation.is_set():
                with self.sustain_service():
                    job = self.wait_for_a_job()
                    job = self.execute_job(job)
                    self.publish_artifacts(job)
                time.sleep(self.sleep_time)
        finally:
            self.logger.warning('Ulta agent is stopped')

    def serve_single_job(self, job_id: str) -> JobResult:
        try:
            job = self.get_job(job_id)
            if not job:
                raise JobNotExecutedError(f'Unable to find cloud job with id {job_id}')
            if job.id != job_id:
                raise JobNotExecutedError(f'Requested cloud job {job_id}, got: {job.id}')
            job = self.execute_job(job)
            self.publish_artifacts(job)
            return job.result()
        except Exception as e:
            self.logger.exception('Job execution failed for test_id %s', dict(test_id=job_id, error=e))
            raise

    def execute_job(self, job: Job) -> Job:
        try:
            self.await_tank_is_ready()
            self.logger.info('Starting test execution %(test_id)s', dict(test_id=job.id))

            job = self.tank_client.prepare_job(job, self._get_job_data_paths(job.test_data_dir))
            self.event_logger.info(
                'Test %(test_id)s prepare step is finished',
                dict(test_id=job.id, internal_id=job.tank_job_id),
            )

            self.logger.info('Waiting for test id(%(test_id)s) to finish', dict(test_id=job.id))
            self.serve_lt_job(job)
            self.logger.info('The test %(test_id)s is finished', dict(test_id=job.id, tank_job_id=job.tank_job_id))
        except JobStoppedError:
            self.logger.warning('Test has been stopped', dict(test_id=job.id))
            self.claim_job_status(job, JobStatus.from_status(AdditionalJobStatus.STOPPED))
        except CancellationRequest as e:
            self.logger.warning('Test has been interrupted due to agent shutdown', dict(test_id=job.id, reason=str(e)))
            self.claim_job_failed(job, f'Job execution has been interrupted on agent. {str(e)}', 'INTERRUPTED')
        except (FailedPrecondition, NotFound) as e:
            self.logger.error(
                'Test has been interrupted due to backend connection reason', dict(test_id=job.id, reason=str(e))
            )
            # this will most likely raise another failed_precondition/not_found error
            # but we should try to report error message anyway
            self.claim_job_failed(job, f'Backend rejected current job: {str(e)}', 'FAILED')
        except TankError as e:
            self.event_logger.error(
                'Test %(test_id)s has been interrupted due to YandexTank error %(error)s',
                dict(test_id=job.id, internal_id=job.tank_job_id, error=str(e)),
            )
            self.claim_job_failed(job, f'Could not run job: {str(e)}', INTERNAL_ERROR_TYPE)
        finally:
            self.tank_client.stop_job()
            self.tank_client.finish()
            self.event_logger.info(
                'Cleanup completed for test %(test_id)s',
                dict(
                    test_id=job.id,
                    internal_id=job.tank_job_id,
                    status=job.status.status,
                    exit_code=job.status.exit_code,
                    error=job.status.error,
                ),
            )
        return job

    def publish_artifacts(self, job: Job):
        with self.override_status(TankStatus.UPLOADING_ARTIFACTS):
            for uploader in self.artifact_uploaders:
                if not uploader.service.can_publish(job):
                    continue
                try:
                    uploader.service.publish_artifacts(job)
                    self.event_logger.info(
                        'Publish artifacts to %(publisher)s completed',
                        dict(publisher=uploader.name, test_id=job.id, internal_id=job.tank_job_id),
                    )
                except CancellationRequest as error:
                    self.claim_post_job_error(
                        job, f'Artifact uploading has been interrupted: {str(error)}', 'ARTIFACT_UPLOADING_FAILED'
                    )
                except Exception as error:
                    self.event_logger.exception(
                        'Failed to publish artifacts to %(publisher)s',
                        dict(publisher=uploader.name, test_id=job.id, internal_id=job.tank_job_id),
                    )
                    self.claim_post_job_error(job, str(error), 'ARTIFACT_UPLOADING_FAILED')

    @contextmanager
    def override_status(self, status: TankStatus):
        try:
            self._override_status = status
            yield
        finally:
            self._override_status = None

    def sustain_job(self):
        exceptions = (*LOADTESTING_UNAVAILABLE_ERRORS, InternalServerError)
        return self._observer.observe(stage='execute test', critical=False, exceptions=exceptions, suppress=True)

    @contextmanager
    def sustain_service(self):
        try:
            yield
        except CancellationRequest:
            self.event_logger.info('Terminating service...')
        except Exception:
            self.event_logger.exception('Unhandled exception occured. Abandoning pending job...')
            return True
