from dataclasses import dataclass
from strenum import StrEnum
from yandextank.common.util import Status


class AdditionalJobStatus(StrEnum):
    JOB_STATUS_UNSPECIFIED = 'JOB_STATUS_UNSPECIFIED'
    STOPPED = 'STOPPED'
    FAILED = 'FAILED'
    AUTOSTOPPED = 'AUTOSTOPPED'


FINISHED_STATUSES_TO_EXIT_CODE_MAPPING = {
    AdditionalJobStatus.AUTOSTOPPED: 20,  # abstract autostop
    AdditionalJobStatus.FAILED: 1,
    Status.TEST_FINISHED.decode('utf-8'): 0,
    AdditionalJobStatus.STOPPED: 0,
}


@dataclass
class JobStatus:
    status: str
    error: str
    error_type: str
    exit_code: int | None = None

    def finished(self) -> bool:
        return self.status in FINISHED_STATUSES_TO_EXIT_CODE_MAPPING

    @classmethod
    def from_status(
        cls,
        status: str | bytes,
        error: str | None = None,
        error_type: str | None = None,
        exit_code: int | str | None = None,
    ):
        if isinstance(status, bytes):
            status = status.decode('utf-8')

        return JobStatus(
            status=status,
            error=error,
            error_type=error_type,
            exit_code=JobStatus._interpret_exit_code(exit_code, status),
        )

    @staticmethod
    def _interpret_exit_code(exit_code: int | str | None, status: str) -> int | None:
        if isinstance(exit_code, int):
            return exit_code
        if isinstance(exit_code, str):
            try:
                return int(exit_code)
            except ValueError:
                # interpret non-zero exitcode as failure
                return 1
        if exit_code is None and status in FINISHED_STATUSES_TO_EXIT_CODE_MAPPING:
            return FINISHED_STATUSES_TO_EXIT_CODE_MAPPING[status]
        return None
