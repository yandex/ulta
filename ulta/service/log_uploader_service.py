import io
import logging
import re
import os

from enum import IntEnum
from pathlib import Path
from ulta.common.cancellation import Cancellation, CancellationType, CancellationRequest
from ulta.common.exceptions import ArtifactUploadError
from ulta.common.job import Job
from ulta.common.interfaces import CloudLoggingClient
from ulta.service.artifact_uploader import ArtifactUploader

# limits are taken from https://cloud.yandex.com/en-ru/docs/logging/concepts/limits
CHUNK_MAX_SIZE = 100
MESSAGE_MAX_LENGTH = 65536


class LogType(IntEnum):
    UNKNOWN = 0
    TANK = 1
    PHANTOM = 2
    PANDORA = 3
    JMETER = 4


class LogUploaderService(ArtifactUploader):
    def __init__(self, cloud_logging_client: CloudLoggingClient, cancellation: Cancellation, logger: logging.Logger):
        self.logger = logger
        self.cancellation = cancellation
        self.cloud_logging_client = cloud_logging_client

    def publish_artifacts(self, job: Job):
        if job.log_group_id and job.artifact_dir_path:
            self.logger.info('Sending logs...')
            self._send_log_file(job, job.artifact_dir_path, LogType.TANK)
            self._send_log_file(job, job.artifact_dir_path, LogType[job.generator.name])

    def _send_log_file(self, job: Job, artifact_dir_path: str, log_type: LogType):
        try:
            self.cancellation.raise_on_set(CancellationType.FORCED)
            if log_file := get_log_file(artifact_dir_path, log_type):
                self._send_log(log_file, log_type, job.log_group_id, job.id)
        except CancellationRequest:
            raise
        except Exception as error:
            raise ArtifactUploadError(
                f'Failed to send log file {log_type.name} from {artifact_dir_path} for job id({job.id}) into log group id({job.log_group_id}): {str(error)}'
            )

    def _send_log(self, log_file: str, log_type: LogType, log_group_id: str, job_id: str):
        if not Path(log_file).is_file():
            self.logger.error('No file for sending log for %s', log_type.name)
            return

        reader = LogReader(log_file, self.logger)
        for chunk in reader.read():
            self.cancellation.raise_on_set(CancellationType.FORCED)
            _ = self.cloud_logging_client.send_log(
                log_group_id, chunk, f'loadtesting.log.{log_type.name.lower()}', resource_id=job_id
            )
        self.logger.debug('Logs were sent.')


def get_log_file(test_dir, log_type):
    if log_type is LogType.TANK:
        return os.path.join(test_dir, 'tank.log')
    elif log_type is LogType.PHANTOM:
        pattern = r'phantom_[^_]*\.log'
    elif log_type is LogType.PANDORA:
        pattern = r'pandora_[^_]*\.log'
    elif log_type == LogType.JMETER:
        pattern = r'jmeter_[^_]*\.log'
    for f in os.listdir(test_dir):
        if re.match(pattern, f):
            return os.path.join(test_dir, f)
    return


class LogReader:
    def __init__(self, log_file, logger: logging.Logger):
        self.logger = logger
        self.log_file = log_file
        self._size_left = 0
        self._lines = None
        self._collector = []

    def read(self):
        try:
            with open(self.log_file, 'r') as f:
                for chunk in self.read_log_data(f):
                    yield chunk
        except Exception:
            self.logger.exception('failed to read log file %s', self.log_file)
            raise

    def _init_chunk(self, message_max_length):
        self._lines = []
        self._size_left = message_max_length
        self._collector.append(self._lines)

    def read_log_data(
        self, reader: io.BufferedReader, chunk_max_size=CHUNK_MAX_SIZE, message_max_length=MESSAGE_MAX_LENGTH
    ):
        def flush():
            if not self._collector:
                return ''
            chunk = self._collector[:chunk_max_size]
            self._collector = self._collector[chunk_max_size:]
            return [''.join(lines) for lines in chunk]

        def sink(lines):
            for line in lines:
                if len(line) > self._size_left:
                    self._init_chunk(message_max_length)
                self._lines.append(line)
                self._size_left -= len(line)

        while line := reader.readline():
            if len(line) > message_max_length:
                self.logger.warning(
                    'log message is exceeding service limit of 64KB per message, sending cut message...'
                )
                sink([line[i : i + message_max_length] for i in range(0, len(line), message_max_length)])
            else:
                sink([line])

            if len(self._collector) > chunk_max_size:
                yield flush()

        while len(self._collector) > 0:
            yield flush()
