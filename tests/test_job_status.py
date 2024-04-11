import pytest
from ulta.common.job_status import JobStatus, AdditionalJobStatus
from yandextank.common.util import Status as TankJobStatus


@pytest.mark.parametrize(
    ('status', 'exp_status', 'exp_exit_code', 'exp_finished'),
    [
        (AdditionalJobStatus.AUTOSTOPPED, 'AUTOSTOPPED', 20, True),
        (AdditionalJobStatus.FAILED, 'FAILED', 1, True),
        (AdditionalJobStatus.JOB_STATUS_UNSPECIFIED, 'JOB_STATUS_UNSPECIFIED', None, False),
        (AdditionalJobStatus.STOPPED, 'STOPPED', 0, True),
    ],
)
@pytest.mark.parametrize(
    'status_transform',
    [
        lambda s: s,
        lambda s: s.name,
    ],
)
def test_job_status_from_additional_status(status, status_transform, exp_status, exp_exit_code, exp_finished):
    s = JobStatus.from_status(status_transform(status))
    assert s.status == exp_status
    assert s.exit_code == exp_exit_code
    assert s.finished() == exp_finished


@pytest.mark.parametrize(
    ('status', 'exp_status', 'exp_exit_code', 'exp_finished'),
    [
        (TankJobStatus.TEST_FINISHED, 'FINISHED', 0, True),
        (TankJobStatus.TEST_FINISHING, 'FINISHING', None, False),
        (TankJobStatus.TEST_INITIATED, 'INITIATED', None, False),
        (TankJobStatus.TEST_POST_PROCESS, 'POST_PROCESS', None, False),
        (TankJobStatus.TEST_PREPARING, 'PREPARING', None, False),
        (TankJobStatus.TEST_RUNNING, 'RUNNING', None, False),
        (TankJobStatus.TEST_WAITING_FOR_A_COMMAND_TO_RUN, 'WAITING_FOR_A_COMMAND_TO_RUN', None, False),
    ],
)
@pytest.mark.parametrize(
    'status_transform',
    [
        lambda s: s,
        lambda s: s.decode('utf-8'),
        lambda s: str(s.decode('utf-8')),
    ],
)
def test_job_status_from_tank_status(status, status_transform, exp_status, exp_exit_code, exp_finished):
    s = JobStatus.from_status(status_transform(status))
    assert s.status == exp_status
    assert s.exit_code == exp_exit_code
    assert s.finished() == exp_finished
