import functools
import json
import logging
import grpc
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
from pathlib import Path
from yandex.cloud.loadtesting.agent.v1 import job_service_pb2, agent_service_pb2

from ulta.yc.backend_client import YCLoadtestingClient, YCJobDataUploaderClient
from ulta.service.tank_client import TankError
from ulta.common.agent import AgentOrigin, AgentInfo
from ulta.common.ammo import Ammo
from ulta.common.cancellation import CancellationRequest
from ulta.common.file_system import FS
from ulta.common.job import Job
from ulta.common.job_status import AdditionalJobStatus, JobStatus
from ulta.common.state import State
from ulta.common.cancellation import Cancellation
from ulta.service.tank_client import TankClient, TankStatus
from ulta.service.service import UltaService
from ulta.service.service_context import LabelContext
from ulta.service.status_reporter import StatusReporter
from ulta.common.exceptions import (
    JobStoppedError,
    JobNotExecutedError,
)
from unittest.mock import patch, MagicMock

from ulta.yc.s3_client import YCS3Client
from yandextank.common.util import Status as TankJobStatus

FAKE_AGENT_VERSION = 'some_version'


@pytest.fixture
def ulta_service():
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
    fs = FS(tmp_dir=Path('/tmp'), tests_dir=Path('/tmp'), lock_dir=Path('/tmp'))

    tank_client = TankClient(logging.getLogger(), fs, job_data_client, 'api_address')
    cancellation = Cancellation()
    state = State()

    with patch('ulta.common.file_system.ensure_dir'):
        yield UltaService(
            state=state,
            loadtesting_client=loadtesting_client,
            tank_client=tank_client,
            s3_client=YCS3Client('storage_url', MagicMock()),
            tmp_dir=fs.tmp_dir,
            sleep_time=0.1,
            artifact_uploaders=MagicMock(),
            cancellation=cancellation,
            label_context=LabelContext(),
        )


@pytest.mark.usefixtures(
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
    ulta_service: UltaService,
):
    patch_tank_client_get_tank_status.return_value = TankStatus.READY_FOR_TEST
    patch_loadtesting_client_get_job.return_value = job_service_pb2.Job(
        id='job123',
        config='{"valid": "json"}',
        data_payload=[job_service_pb2.TestDataEntry(name='ammo', is_transient=True)],
    )
    with pytest.raises(JobNotExecutedError):
        ulta_service.serve_single_job('job-123')


@pytest.mark.usefixtures(
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
    ulta_service: UltaService,
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
    result = ulta_service.serve_single_job('job-123')

    patch_tank_client_prepare_job.assert_called_once()
    patch_tank_client_finish.assert_called()
    assert result.exit_code == 0


def test_cancellation(
    patch_tank_client_get_tank_status,
    patch_loadtesting_client_get_job,
    check_threads_leak,
    ulta_service: UltaService,
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
    ulta_service.cancellation = cancellation
    ulta_service.serve()
    assert cancellation.is_set()


@pytest.mark.parametrize(
    'claim_status_error',
    [
        FailedPrecondition(''),
        NotFound(''),
    ],
)
def test_cancellation_from_reporter(
    patch_tank_client_get_tank_status,
    patch_loadtesting_client_claim_tank_status,
    patch_loadtesting_client_get_job,
    check_threads_leak,
    claim_status_error,
    ulta_service: UltaService,
):
    patch_tank_client_get_tank_status.return_value = TankStatus.READY_FOR_TEST
    patch_loadtesting_client_get_job.side_effect = NotFound('')
    patch_loadtesting_client_claim_tank_status.side_effect = claim_status_error
    reporter = StatusReporter(
        logging.getLogger(),
        ulta_service.tank_client,
        ulta_service.loadtesting_client,
        ulta_service.cancellation,
        State(),
    )
    with reporter.run():
        ulta_service.serve()
    patch_loadtesting_client_claim_tank_status.assert_called_with(
        str(TankStatus.STOPPED.name),
        "The backend doesn't know this agent: agent has been deleted or account is missing loadtesting.generatorClient role.",
    )


@pytest.mark.parametrize(
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
    ulta_service: UltaService,
):
    ammos = ulta_service._extract_ammo(job_message, tmp_path)
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


@pytest.mark.usefixtures(
    'patch_loadtesting_client_claim_tank_status',
    'patch_loadtesting_s3_client_download',
    'patch_loadtesting_client_download_transient_ammo',
)
@pytest.mark.parametrize(
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
    ulta_service: UltaService,
    job,
    job_id,
):
    patch_tank_client_get_tank_status.return_value = tank_status
    patch_loadtesting_client_get_job.return_value = job
    res_job = ulta_service.get_job(job_id)
    assert res_job is not None
    assert res_job.id == job.id
    assert res_job.config == json.loads(job.config)
    assert len(res_job.ammos) == 1
    assert {p.name for p in job.data_payload} == {ammo.name for ammo in res_job.ammos}


def test_get_job_not_found(check_threads_leak, ulta_service: UltaService):
    with patch.object(YCLoadtestingClient, 'get_job', side_effect=NotFound('')):
        job = ulta_service.get_job()
        assert job is None
        assert ulta_service._observer._state.ok is True


def test_wait_for_a_job_error(check_threads_leak, ulta_service: UltaService):
    with patch.object(YCLoadtestingClient, 'get_job', side_effect=FailedPrecondition('')):
        with pytest.raises(FailedPrecondition):
            _ = ulta_service.wait_for_a_job()
        assert ulta_service._observer._state.ok is False


@pytest.mark.usefixtures(
    'patch_tank_client_get_tank_status',
    'patch_loadtesting_client_claim_tank_status',
    'patch_loadtesting_client_claim_job_status',
    'patch_tank_client_run_job',
)
@pytest.mark.parametrize(
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
    ulta_service: UltaService,
    job_status,
):
    patch_tank_client_get_job_status.return_value = JobStatus.from_status(job_status)
    patch_loadtesting_client_get_job_signal.return_value = job_service_pb2.JobSignalResponse(
        signal=job_service_pb2.JobSignalResponse.Signal.Value('SIGNAL_UNSPECIFIED')
    )
    job = Job(id='123', config={'plugin': {'enabled': True}}, tank_job_id='123')
    patch_tank_client_prepare_job.return_value = job
    ulta_service.serve_lt_job(job)
    patch_loadtesting_client_claim_job_status.assert_called_with('123', job_status, None, None)


@pytest.mark.usefixtures(
    'patch_tank_client_get_tank_status',
    'patch_loadtesting_client_claim_tank_status',
    'patch_loadtesting_client_claim_job_status',
    'patch_tank_client_run_job',
)
def test_serve_job_stop(
    patch_loadtesting_client_get_job_signal,
    check_threads_leak,
    ulta_service: UltaService,
):
    patch_loadtesting_client_get_job_signal.return_value = job_service_pb2.JobSignalResponse(
        signal=job_service_pb2.JobSignalResponse.Signal.Value('STOP')
    )
    job = Job(id='123', config={'plugin': {'enabled': True}})
    with pytest.raises(JobStoppedError):
        ulta_service.serve_lt_job(job)


@pytest.mark.usefixtures(
    'patch_tank_client_get_tank_status',
    'patch_loadtesting_client_claim_tank_status',
    'patch_loadtesting_client_claim_job_status',
    'patch_tank_client_run_job',
)
@pytest.mark.parametrize(
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
    ulta_service: UltaService,
    raise_ex,
    expected_status_args,
    expected_exit_code,
):
    patch_ulta_serve_lt_job.side_effect = raise_ex
    patch_tank_client_get_job_status.return_value = JobStatus.from_status(TankJobStatus.TEST_RUNNING)
    job = Job(id='123', config={'plugin': {'enabled': True}}, tank_job_id='123')
    patch_tank_client_prepare_job.return_value = job
    job_got = ulta_service._execute_job(job)
    patch_loadtesting_client_claim_job_status.assert_called_with('123', *expected_status_args)
    patch_tank_client_stop_job.assert_called()
    assert job_got.status.exit_code == expected_exit_code


@pytest.mark.usefixtures(
    'patch_tank_client_get_tank_status',
    'patch_loadtesting_client_claim_tank_status',
    'patch_loadtesting_client_claim_job_status',
    'patch_tank_client_run_job',
)
@pytest.mark.parametrize(
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
    ulta_service: UltaService,
    call_function,
    exception_to_raise,
):
    job = Job(id='123', config={'plugin': {'enabled': True}})

    with patch.object(YCLoadtestingClient, call_function) as m:
        m.side_effect = exception_to_raise('')
        patch_tank_client_get_job_status.return_value = JobStatus.from_status(TankJobStatus.TEST_RUNNING)
        if call_function != 'get_job_signal':
            patch_loadtesting_client_get_job_signal.return_value = job_service_pb2.JobSignalResponse(
                signal=job_service_pb2.JobSignalResponse.Signal.Value('SIGNAL_UNSPECIFIED')
            )
        with pytest.raises(exception_to_raise):
            ulta_service.serve_lt_job(job)


@pytest.mark.usefixtures(
    'patch_tank_client_get_tank_status',
    'patch_loadtesting_client_claim_job_status',
    'patch_tank_client_finish',
    'patch_tank_client_run_job',
)
@pytest.mark.parametrize('mock_failure', ['serve_lt_signal', 'claim_job_status'])
@pytest.mark.parametrize(
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
    ulta_service: UltaService,
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
        job = Job(id='123', config={'plugin': {'enabled': True}})
        ulta_service.sleep_time = 0.01
        ulta_service.serve_lt_job(job)

    assert not scenario


@pytest.mark.usefixtures(
    'patch_tank_client_get_tank_status',
    'patch_tank_client_finish',
    'patch_tank_client_run_job',
)
def test_serve_job_sustain_prepare_job_error(
    patch_tank_client_stop_job,
    patch_loadtesting_client_claim_job_status,
    patch_tank_client_prepare_job,
    ulta_service: UltaService,
    check_threads_leak,
):
    patch_tank_client_prepare_job.side_effect = TankError()
    job = Job(id='123', config={'plugin': {'enabled': True}}, tank_job_id='123')
    ulta_service._execute_job(job)

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
def test_supress_non_critical_errors_strategy_sustain_job_raises_critical_errors(
    ulta_service: UltaService, expected_exception
):
    with pytest.raises(expected_exception):
        with ulta_service.sustain_job('job_id'):
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
def test_supress_non_critical_errors_strategy_sustain_job(ulta_service: UltaService, expected_exception):
    with ulta_service.sustain_job('job_id'):
        raise expected_exception


@pytest.mark.parametrize(
    'error',
    [
        Exception(),
        RuntimeError(),
    ],
)
def test_publish_artifacts_raise_no_error(ulta_service: UltaService, error):
    job = Job(
        id='123',
        config={'pandora': {'enabled': True}},
        tank_job_id='123',
        log_group_id='loggroup',
        artifact_dir_path='/tmp',
    )
    a1, a2 = MagicMock(), MagicMock()
    a1.service.publish_artifacts.side_effect = error
    a2.service.publish_artifacts.side_effect = error
    ulta_service.artifact_uploaders = [a1, a2]
    ulta_service._publish_artifacts(job)
    a1.service.publish_artifacts.assert_called()
    a2.service.publish_artifacts.assert_called()


@pytest.mark.parametrize(
    'test_case',
    (
        (
            'stub_agent',
            'ClaimStatus',
            functools.partial(
                YCLoadtestingClient.claim_tank_status, tank_status='STATUS_UNSPECIFIED', status_message=''
            ),
            agent_service_pb2.ClaimAgentStatusResponse(code=0),
        ),
        (
            'stub_job',
            'ClaimStatus',
            functools.partial(YCLoadtestingClient.claim_job_status, job_id='jid', job_status=0),
            job_service_pb2.ClaimJobStatusResponse(code=0),
        ),
        ('stub_job', 'Get', functools.partial(YCLoadtestingClient.get_job, job_id='jid'), job_service_pb2.Job()),
        (
            'stub_job',
            'GetSignal',
            functools.partial(YCLoadtestingClient.get_job_signal, job_id='jid'),
            job_service_pb2.JobSignalResponse(
                signal=job_service_pb2.JobSignalResponse.Signal.Value('SIGNAL_UNSPECIFIED')
            ),
        ),
    ),
)
@pytest.mark.parametrize(
    'tested_error',
    [
        grpc.StatusCode.UNKNOWN,
        grpc.StatusCode.PERMISSION_DENIED,
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.UNAUTHENTICATED,
    ],
)
def test_retry_lt_errors(test_case, tested_error):
    mocked_obj, mocked_func, tested_func, expected_result = test_case

    err = grpc.RpcError()
    err.code = lambda: tested_error
    scenario_actions = [err] * 2 + [expected_result]

    def scenario(*args, **kwargs):
        if scenario_actions:
            value = scenario_actions.pop(0)
            if isinstance(value, Exception):
                raise value
            return value
        raise RuntimeError()

    agent = AgentInfo(
        id='agent_id',
        origin=AgentOrigin.COMPUTE_LT_CREATED,
        version=FAKE_AGENT_VERSION,
        folder_id='some_folder_id',
        name='some_name',
    )
    lt_client = YCLoadtestingClient(MagicMock(), MagicMock(), agent)

    with patch.object(getattr(lt_client, mocked_obj), mocked_func, scenario) as mock_func:
        mock_func.side_effect = scenario
        result = tested_func(lt_client)

    assert result == expected_result.code if isinstance(result, int) else expected_result
    assert not scenario_actions
