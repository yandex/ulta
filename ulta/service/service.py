import json
import logging
import os
import time
from contextlib import contextmanager
from typing import Iterable

from ulta.common.ammo import Ammo
from ulta.common.cancellation import Cancellation, CancellationRequest
from ulta.common.interfaces import LoadtestingClient, S3Client, NamedService
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
from ulta.common.job import Job, JobResult, ArtifactSettings
from ulta.common.job_status import AdditionalJobStatus, JobStatus
from ulta.service.artifact_uploader import ArtifactUploader
from ulta.service.tank_client import TankClient, TankStatus, INTERNAL_ERROR_TYPE

LOCK_DIR = '/var/lock'
FINISHED_FILE = 'finish_status.yaml'


class UltaService:
    def __init__(
        self,
        logger: logging.Logger,
        loadtesting_client: LoadtestingClient,
        tank_client: TankClient,
        s3_client: S3Client,
        work_dir: str,
        sleep_time: float,
        artifact_uploaders: Iterable[NamedService[ArtifactUploader]],
        cancellation: Cancellation,
        max_waiting_time: int = 300,
    ):
        self.logger = logger
        self.cancellation = cancellation
        self.work_dir = work_dir

        self.loadtesting_client = loadtesting_client
        self.s3_client = s3_client
        self.tank_client = tank_client
        self.sleep_time = sleep_time
        self.tank_status_report_delay = sleep_time
        self.job_pooling_delay = sleep_time
        self.artifact_uploaders = artifact_uploaders
        self.max_waiting_time = max_waiting_time
        self._override_status: TankStatus | None = None

    def get_tank_status(self) -> TankStatus:
        if self._override_status is not None:
            return self._override_status
        return self.tank_client.get_tank_status()

    def _extract_ammo(self, job_message, test_data_dir) -> list[Ammo]:
        res = []
        for payload_entry in job_message.data_payload:
            ammo_name = payload_entry.name
            if not ammo_name:
                self.logger.warning('Test data specified with no name.')
                raise InvalidJobDataError('Test data specified with no name.')

            ammo_file_path = os.path.join(test_data_dir, ammo_name.strip('/'))
            ammo_file_path = os.path.normpath(ammo_file_path)
            if os.path.commonpath((test_data_dir, ammo_file_path)) != os.path.normpath(test_data_dir):
                self.logger.error('Cannot write ammo file to %s', ammo_file_path)
                raise InvalidJobDataError('Invalid test data name')

            if payload_entry.is_transient:
                self.logger.info('Downloading transient ammo job_id=%s, name=%s', job_message.id, ammo_name)
                self.loadtesting_client.download_transient_ammo(
                    job_id=job_message.id,
                    ammo_name=ammo_name,
                    path_to_download=ammo_file_path,
                )
            else:
                self.logger.info(
                    'Downloading s3 file from %s/%s',
                    payload_entry.storage_object.object_storage_bucket,
                    payload_entry.storage_object.object_storage_filename,
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
            self.logger.exception('Failed to set %s error to job: %s', error_type, str(e))

    def claim_job_status(self, job: Job, status: JobStatus):
        job.update_status(status)
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

    def get_job(self, job_id: str | None = None) -> Job | None:
        try:
            job_message = self.loadtesting_client.get_job(job_id)
        except NotFound:
            self.logger.info('No pending jobs for agent')
            return None

        if job_message is None or not job_message.id:
            return None

        job = Job(id=job_message.id)
        try:
            job.log_group_id = job_message.logging_log_group_id
            job.config = json.loads(job_message.config)
            job.test_data_dir = os.path.abspath(os.path.join(self.work_dir, f'test_data_{job_message.id}'))
            job.upload_artifact_settings = self.extract_artifact_settings(job_message)

            os.makedirs(job.test_data_dir, exist_ok=True)
            job.ammos = self._extract_ammo(job_message, job.test_data_dir)
            return job
        except json.JSONDecodeError as error:
            self.logger.exception('Invalid job config format')
            self.claim_job_failed(job, f'Invalid job config:{str(error)}', 'JOB_CONFIG')
        except (
            ObjectStorageError,
            ClientError,
            InvalidJobDataError,
        ) as error:
            self.logger.exception('Error loading test data')
            self.claim_job_failed(job, f'Error loading test data: {str(error)})', 'JOB_AMMO')
        except Exception as error:
            self.logger.exception('Unknown exception')
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
        while not self.cancellation.is_set():
            with self.sustain_service():
                job = self.wait_for_a_job()
                job = self.execute_job(job)
                self.publish_artifacts(job)
            time.sleep(self.sleep_time)

    def serve_single_job(self, job_id: str) -> JobResult:
        job = self.get_job(job_id)
        if not job:
            raise JobNotExecutedError(f'Unable to find cloud job with id {job_id}')
        if job.id != job_id:
            raise JobNotExecutedError(f'Requested cloud job {job_id}, got: {job.id}')
        job = self.execute_job(job)
        self.publish_artifacts(job)
        return job.result()

    def execute_job(self, job: Job) -> Job:
        try:
            self.await_tank_is_ready()

            job = self.tank_client.prepare_job(job, self._get_job_data_paths(job.test_data_dir))
            self.logger.info('Prepared job id(%s), tank_job_id(%s)', job.id, job.tank_job_id)

            self.logger.info('waiting for job id(%s) to finish', job.id)
            self.serve_lt_job(job)
            self.logger.info('The job id(%s), tank_job_id(%s) is finished', job.id, job.tank_job_id)
        except JobStoppedError:
            self.claim_job_status(job, JobStatus.from_status(AdditionalJobStatus.STOPPED))
        except CancellationRequest as e:
            self.claim_job_failed(job, f'Job execution has been interrupted on agent. {str(e)}', 'INTERRUPTED')
        except (FailedPrecondition, NotFound) as e:
            # this will most likely raise another failed_precondition/not_found error
            # but we should try to report error message anyway
            self.claim_job_failed(job, f'Backend rejected current job: {str(e)}', 'FAILED')
        except TankError as e:
            self.claim_job_failed(job, f'Could not run job: {str(e)}', INTERNAL_ERROR_TYPE)
        finally:
            self.tank_client.stop_job()
            self.tank_client.finish()
        return job

    def publish_artifacts(self, job: Job):
        with self.override_status(TankStatus.UPLOADING_ARTIFACTS):
            for uploader in self.artifact_uploaders:
                try:
                    uploader.service.publish_artifacts(job)
                except CancellationRequest as error:
                    self.claim_post_job_error(
                        job, f'Artifact uploading has been interrupted: {str(error)}', 'ARTIFACT_UPLOADING_FAILED'
                    )
                except Exception as error:
                    self.logger.exception('Failed to publish artifacts to %s', uploader.name)
                    self.claim_post_job_error(job, str(error), 'ARTIFACT_UPLOADING_FAILED')

    @contextmanager
    def override_status(self, status: TankStatus):
        try:
            self._override_status = status
            yield
        finally:
            self._override_status = None

    @contextmanager
    def sustain_job(self):
        try:
            yield
        except (*LOADTESTING_UNAVAILABLE_ERRORS, InternalServerError):
            self.logger.exception('Request to backend failed. Retrying...')
            return True

    @contextmanager
    def sustain_service(self):
        try:
            yield
        except CancellationRequest:
            self.logger.info('Received interrupt signal.')
        except Exception:
            self.logger.exception('Unandled exception occured. Abandoning pending job...')
            return True
