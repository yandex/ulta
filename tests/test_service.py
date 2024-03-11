import json
import pytest
from google.api_core.exceptions import (
    ServiceUnavailable,
    GatewayTimeout,
    TooManyRequests,
    FailedPrecondition,
    InternalServerError,
    InvalidArgument,
    NotFound,
)

from pytest import mark
from yandex.cloud.loadtesting.agent.v1 import job_service_pb2

from ulta.yc.backend_client import YCLoadtestingClient, YCJobDataUploaderClient
from ulta.service.tank_client import TankError
from ulta.common.agent import AgentOrigin, AgentInfo
from ulta.common.ammo import Ammo
from ulta.common.cancellation import CancellationRequest
from ulta.common.job import Job
from ulta.common.job_status import AdditionalJobStatus, JobStatus
from ulta.common.cancellation import Cancellation
from ulta.service.tank_client import TankClient, TankStatus
from ulta.service.service import UltaService
from ulta.service.status_reporter import StatusReporter
from ulta.common.exceptions import (
    JobStoppedError,
    JobNotExecutedError,
)
from unittest.mock import patch, MagicMock

from ulta.yc.s3_client import YCS3Client
from yandextank.common.util import Status as TankJobStatus

FAKE_AGENT_VERSION = 'some_version'


def ulta_service(sleep_time=1):
    agent = AgentInfo(
        id='agent_id',
        origin=AgentOrigin.COMPUTE_LT_CREATED,
        version=FAKE_AGENT_VERSION,
        folder_id='some_folder_id',
        name='some_name',
    )
    loadtesting_client = YCLoadtestingClient(
        MagicMock(),
        MagicMock(),
        agent,
    )
    job_data_client = YCJobDataUploaderClient(
        MagicMock(),
        MagicMock(),
        agent,
    )

    tank_client = TankClient(MagicMock(), '/tmp', '/var/lock', job_data_client, 'api_address')
    cancellation = Cancellation()

    return UltaService(
        MagicMock(),
        loadtesting_client,
        tank_client,
        YCS3Client('storage_url', MagicMock()),
        '/tmp',
        sleep_time,
        MagicMock(),
        cancellation,
    )


@mark.usefixtures(
    'patch_tank_client_get_tank_status',
    'patch_loadtesting_client_claim_tank_status',
    'patch_loadtesting_client_claim_job_status',
    'patch_loadtesting_client_download_transient_ammo',
    'patch_tank_client_run_job',
)
def test_serve_single_job_doesnt_run_if_job_is_mismatch(
    patch_loadtesting_client_get_job,
    patch_tank_client_get_tank_status,
    check_threads_leak,
):
    patch_tank_client_get_tank_status.return_value = TankStatus.READY_FOR_TEST
    patch_loadtesting_client_get_job.return_value = job_service_pb2.Job(
        id='job123',
        config='{"valid": "json"}',
        data_payload=[job_service_pb2.TestDataEntry(name='ammo', is_transient=True)],
    )
    with pytest.raises(JobNotExecutedError):
        ulta_service().serve_single_job('job-123')


@mark.usefixtures(
    'patch_tank_client_get_tank_status',
    'patch_loadtesting_client_claim_tank_status',
    'patch_loadtesting_client_claim_job_status',
    'patch_loadtesting_client_download_transient_ammo',
    'patch_tank_client_run_job',
)
def test_serve_single_job(
    patch_tank_client_get_tank_status,
    patch_loadtesting_client_get_job,
    patch_loadtesting_client_get_job_signal,
    patch_tank_client_finish,
    patch_tank_client_prepare_job,
    patch_tank_client_get_job_status,
    check_threads_leak,
):
    patch_tank_client_get_tank_status.return_value = TankStatus.READY_FOR_TEST
    patch_loadtesting_client_get_job.return_value = job_service_pb2.Job(
        id='job-123',
        config='{"valid": "json"}',
        data_payload=[job_service_pb2.TestDataEntry(name='ammo', is_transient=True)],
    )
    patch_tank_client_get_job_status.return_value = JobStatus.from_status(TankJobStatus.TEST_FINISHED)
    patch_loadtesting_client_get_job_signal.return_value = job_service_pb2.JobSignalResponse(
        signal=job_service_pb2.JobSignalResponse.Signal.Value('SIGNAL_UNSPECIFIED')
    )
    patch_tank_client_prepare_job.return_value = Job('job-123', tank_job_id='job-123')
    result = ulta_service().serve_single_job('job-123')

    patch_tank_client_prepare_job.assert_called_once()
    patch_tank_client_finish.assert_called()
    assert result.exit_code == 0


def test_cancellation(
    patch_tank_client_get_tank_status,
    patch_loadtesting_client_get_job,
    check_threads_leak,
):
    cancellation = Cancellation()
    scenario = iter([NotFound('')] * 6)

    def test_scenario(*args):
        try:
            raise next(scenario)
        except StopIteration:
            cancellation.notify('stop')

    patch_tank_client_get_tank_status.return_value = TankStatus.READY_FOR_TEST
    patch_loadtesting_client_get_job.side_effect = test_scenario
    service = ulta_service()
    service.cancellation = cancellation
    service.serve()


@mark.parametrize(
    'claim_status_error',
    [
        FailedPrecondition(''),
        NotFound(''),
    ],
)
def test_cancellation_from_reporter(
    patch_tank_client_get_tank_status,
    patch_status_reporter_report_tank_status,
    patch_loadtesting_client_get_job,
    check_threads_leak,
    claim_status_error,
):
    patch_tank_client_get_tank_status.return_value = TankStatus.READY_FOR_TEST
    patch_loadtesting_client_get_job.side_effect = NotFound('')
    patch_status_reporter_report_tank_status.side_effect = claim_status_error
    service = ulta_service()
    reporter = StatusReporter(MagicMock(), service.tank_client, service.loadtesting_client, service.cancellation)
    with reporter.run():
        service.serve()
    patch_status_reporter_report_tank_status.assert_called_with(TankStatus.STOPPED)


@mark.parametrize(
    'job_message, expected_ammo, expect_s3, expect_transient',
    [
        (
            job_service_pb2.Job(
                data_payload=[
                    job_service_pb2.TestDataEntry(
                        name='some_name',
                        storage_object=job_service_pb2.StorageObject(
                            object_storage_bucket='bucket',
                            object_storage_filename='some_name',
                        ),
                    )
                ]
            ),
            Ammo('some_name', '/dir/some_name'),
            True,
            False,
        ),
        (
            job_service_pb2.Job(data_payload=[job_service_pb2.TestDataEntry(name='some_name', is_transient=True)]),
            Ammo('some_name', '/dir/some_name'),
            False,
            True,
        ),
        (job_service_pb2.Job(), None, False, False),
    ],
)
def test__extract_ammo(
    tmp_path,
    patch_loadtesting_s3_client_download,
    patch_loadtesting_client_download_transient_ammo,
    check_threads_leak,
    job_message,
    expected_ammo,
    expect_s3,
    expect_transient,
):
    ammos = ulta_service()._extract_ammo(job_message, tmp_path)
    if expected_ammo:
        assert ammos[0].name == expected_ammo.name
    else:
        assert not ammos

    if expect_s3:
        patch_loadtesting_s3_client_download.assert_called_once()
    else:
        patch_loadtesting_s3_client_download.assert_not_called()

    if expect_transient:
        patch_loadtesting_client_download_transient_ammo.assert_called_once()
    else:
        patch_loadtesting_client_download_transient_ammo.assert_not_called()


@mark.usefixtures(
    'patch_loadtesting_client_claim_tank_status',
    'patch_loadtesting_s3_client_download',
    'patch_loadtesting_client_download_transient_ammo',
)
@mark.parametrize(
    ('tank_status', 'job', 'job_id'),
    [
        (
            TankStatus.READY_FOR_TEST,
            job_service_pb2.Job(
                id='123',
                config='{"valid": "json"}',
                data_payload=[job_service_pb2.TestDataEntry(name='ammo', is_transient=True)],
            ),
            None,
        ),
        (
            TankStatus.READY_FOR_TEST,
            job_service_pb2.Job(
                id='123',
                config='{"valid": "json"}',
                data_payload=[
                    job_service_pb2.TestDataEntry(
                        name='s3',
                        is_transient=False,
                        storage_object=job_service_pb2.StorageObject(
                            object_storage_bucket='bucket',
                            object_storage_filename='s3',
                        ),
                    )
                ],
            ),
            None,
        ),
        (
            TankStatus.READY_FOR_TEST,
            job_service_pb2.Job(
                id='123',
                config='{"valid": "json"}',
                data_payload=[job_service_pb2.TestDataEntry(name='ammo', is_transient=True)],
            ),
            '123',
        ),
    ],
)
def test_get_job(
    patch_tank_client_get_tank_status,
    patch_loadtesting_client_get_job,
    tank_status,
    check_threads_leak,
    job,
    job_id,
):
    patch_tank_client_get_tank_status.return_value = tank_status
    patch_loadtesting_client_get_job.return_value = job
    res_job = ulta_service().get_job(job_id)
    assert res_job.id == job.id
    assert res_job.config == json.loads(job.config)
    assert len(res_job.ammos) == 1
    assert {p.name for p in job.data_payload} == {ammo.name for ammo in res_job.ammos}


def test_get_job_not_found(check_threads_leak):
    with patch.object(YCLoadtestingClient, 'get_job', side_effect=NotFound('')):
        job = ulta_service().get_job()
        assert job is None


def test_get_job_error(check_threads_leak):
    with patch.object(YCLoadtestingClient, 'get_job', side_effect=FailedPrecondition('')):
        with pytest.raises(FailedPrecondition):
            _ = ulta_service().get_job()


@mark.usefixtures(
    'patch_tank_client_get_tank_status',
    'patch_loadtesting_client_claim_tank_status',
    'patch_loadtesting_client_claim_job_status',
    'patch_tank_client_run_job',
)
@mark.parametrize(
    ('job_status'),
    [
        (AdditionalJobStatus.FAILED),
        (AdditionalJobStatus.AUTOSTOPPED),
        ('FINISHED'),
    ],
)
def test_serve_job(
    patch_tank_client_get_job_status,
    patch_loadtesting_client_claim_job_status,
    patch_loadtesting_client_get_job_signal,
    patch_tank_client_prepare_job,
    check_threads_leak,
    job_status,
):
    patch_tank_client_get_job_status.return_value = JobStatus.from_status(job_status)
    patch_loadtesting_client_get_job_signal.return_value = job_service_pb2.JobSignalResponse(
        signal=job_service_pb2.JobSignalResponse.Signal.Value('SIGNAL_UNSPECIFIED')
    )
    job = Job(id='123', config='config', tank_job_id='123')
    patch_tank_client_prepare_job.return_value = job
    ulta_service().serve_lt_job(job)
    patch_loadtesting_client_claim_job_status.assert_called_with('123', job_status, None, None)


@mark.usefixtures(
    'patch_tank_client_get_tank_status',
    'patch_loadtesting_client_claim_tank_status',
    'patch_loadtesting_client_claim_job_status',
    'patch_tank_client_run_job',
)
def test_serve_job_stop(
    patch_loadtesting_client_get_job_signal,
    check_threads_leak,
):
    patch_loadtesting_client_get_job_signal.return_value = job_service_pb2.JobSignalResponse(
        signal=job_service_pb2.JobSignalResponse.Signal.Value('STOP')
    )
    job = Job(id='123', config='config')
    with pytest.raises(JobStoppedError):
        ulta_service().serve_lt_job(job)


@mark.usefixtures(
    'patch_tank_client_get_tank_status',
    'patch_loadtesting_client_claim_tank_status',
    'patch_loadtesting_client_claim_job_status',
    'patch_tank_client_run_job',
)
@mark.parametrize(
    'raise_ex, expected_status_args, expected_exit_code',
    [
        (JobStoppedError(), ['STOPPED', None, None], 0),
        (CancellationRequest(), ['FAILED', 'Job execution has been interrupted on agent. ', 'INTERRUPTED'], 1),
        (FailedPrecondition(''), ['FAILED', 'Backend rejected current job: 400 ', 'FAILED'], 1),
    ],
)
def test_claim_job_status_on_errors(
    patch_tank_client_get_job_status,
    patch_tank_client_stop_job,
    patch_loadtesting_client_claim_job_status,
    patch_tank_client_prepare_job,
    patch_ulta_serve_lt_job,
    check_threads_leak,
    raise_ex,
    expected_status_args,
    expected_exit_code,
):
    patch_ulta_serve_lt_job.side_effect = raise_ex
    patch_tank_client_get_job_status.return_value = JobStatus.from_status(TankJobStatus.TEST_RUNNING)
    job = Job(id='123', config='config', tank_job_id='123')
    patch_tank_client_prepare_job.return_value = job
    job_got = ulta_service().execute_job(job)
    patch_loadtesting_client_claim_job_status.assert_called_with('123', *expected_status_args)
    patch_tank_client_stop_job.assert_called()
    assert job_got.status.exit_code == expected_exit_code


@mark.usefixtures(
    'patch_tank_client_get_tank_status',
    'patch_loadtesting_client_claim_tank_status',
    'patch_loadtesting_client_claim_job_status',
    'patch_tank_client_run_job',
)
@mark.parametrize(
    ('call_function', 'exception_to_raise'),
    [
        ('claim_job_status', NotFound),
        ('claim_job_status', FailedPrecondition),
        ('get_job_signal', NotFound),
    ],
)
def test_serve_job_error(
    patch_tank_client_get_job_status,
    patch_loadtesting_client_get_job_signal,
    check_threads_leak,
    call_function,
    exception_to_raise,
):
    job = Job(id='123', config='config')

    with patch.object(YCLoadtestingClient, call_function) as m:
        m.side_effect = exception_to_raise('')
        patch_tank_client_get_job_status.return_value = JobStatus.from_status(TankJobStatus.TEST_RUNNING)
        if call_function != 'get_job_signal':
            patch_loadtesting_client_get_job_signal.return_value = job_service_pb2.JobSignalResponse(
                signal=job_service_pb2.JobSignalResponse.Signal.Value('SIGNAL_UNSPECIFIED')
            )
        with pytest.raises(exception_to_raise):
            ulta_service().serve_lt_job(job)


@mark.usefixtures(
    'patch_tank_client_get_tank_status',
    'patch_loadtesting_client_claim_job_status',
    'patch_tank_client_finish',
    'patch_tank_client_run_job',
)
@mark.parametrize('mock_failure', ['serve_lt_signal', 'claim_job_status'])
@mark.parametrize(
    'scenario',
    [
        [InternalServerError('internal error')],
        [GatewayTimeout('conn error')],
        [
            GatewayTimeout('conn error'),
            ServiceUnavailable('conn error 2'),
            None,
            None,
            GatewayTimeout('conn error 3'),
            TooManyRequests('internal error'),
            None,
            InternalServerError('internal error'),
            GatewayTimeout('conn error 3'),
        ],
    ],
)
def test_serve_job_sustain_non_critical_lt_errors(
    patch_tank_client_get_job_status,
    patch_loadtesting_client_get_job_signal,
    check_threads_leak,
    mock_failure,
    scenario,
):
    def test_scenario(*args):
        try:
            ex = scenario.pop()
            if ex is not None:
                raise ex
        except IndexError:
            patch_tank_client_get_job_status.return_value = JobStatus.from_status(TankJobStatus.TEST_FINISHED)
            return

    patch_loadtesting_client_get_job_signal.return_value = job_service_pb2.JobSignalResponse(
        signal=job_service_pb2.JobSignalResponse.Signal.Value('SIGNAL_UNSPECIFIED')
    )
    patch_tank_client_get_job_status.return_value = JobStatus.from_status(TankJobStatus.TEST_RUNNING)
    with patch.object(UltaService, mock_failure, test_scenario) as mock:
        mock.side_effect = test_scenario
        job = Job(id='123', config='config')
        ulta_service(sleep_time=0.1).serve_lt_job(job)

    assert not scenario


@mark.usefixtures(
    'patch_tank_client_get_tank_status',
    'patch_tank_client_finish',
    'patch_tank_client_run_job',
)
def test_serve_job_sustain_prepare_job_error(
    patch_tank_client_stop_job,
    patch_loadtesting_client_claim_job_status,
    patch_tank_client_prepare_job,
    check_threads_leak,
):
    patch_tank_client_prepare_job.side_effect = TankError()
    job = Job(id='123', config='config', tank_job_id='123')
    ulta_service().execute_job(job)

    patch_loadtesting_client_claim_job_status.assert_called_with(
        '123', AdditionalJobStatus.FAILED, 'Could not run job: ', 'internal'
    )
    patch_tank_client_stop_job.assert_called()


@pytest.mark.parametrize(
    'expected_exception',
    [
        FailedPrecondition,
        InvalidArgument,
        Exception,
        RuntimeError,
    ],
)
def test_supress_non_critical_errors_strategy_sustain_job_raises_critical_errors(expected_exception):
    with pytest.raises(expected_exception):
        service = ulta_service()
        with service.sustain_job():
            raise expected_exception('')


@pytest.mark.parametrize(
    'expected_exception',
    [
        InternalServerError(''),
        ServiceUnavailable(''),
        GatewayTimeout(''),
        TooManyRequests(''),
    ],
)
def test_supress_non_critical_errors_strategy_sustain_job(expected_exception):
    service = ulta_service()
    with service.sustain_job():
        raise expected_exception


@pytest.mark.parametrize(
    'error',
    [
        Exception(),
        RuntimeError(),
    ],
)
def test_publish_artifacts_raise_no_error(error):
    job = Job(
        id='123',
        config={'pandora': {'enabled': True}},
        tank_job_id='123',
        log_group_id='loggroup',
        artifact_dir_path='/tmp',
    )
    service = ulta_service()
    a1, a2 = MagicMock(), MagicMock()
    a1.service.publish_artifacts.side_effect = error
    a2.service.publish_artifacts.side_effect = error
    service.artifact_uploaders = [a1, a2]
    service.publish_artifacts(job)
    a1.service.publish_artifacts.assert_called()
    a2.service.publish_artifacts.assert_called()
