from ulta.service.command import run_serve
from ulta.service.service import UltaService
from ulta.common.config import UltaConfig
from ulta.common.cancellation import Cancellation
from ulta.common.file_system import FS
from ulta.common.job import JobResult
from ulta.common.interfaces import ClientFactory, TransportFactory

import logging
from unittest import mock


@mock.patch.object(TransportFactory, 'get')
@mock.patch.object(UltaService, 'serve_single_job')
def test_run_serve_smoke(serve_single_job, transport_factory, fs_mock: FS):
    config = UltaConfig(
        command='',
        environment='DEFAULT',
        transport='mocked',
        backend_service_url='',
        iam_service_url='',
        logging_service_url='',
        object_storage_url='',
        work_dir='/tmp/test',
        lock_dir='/tmp/test',
        request_frequency=1,
        instance_lt_created=False,
        folder_id='some_folder_id',
        agent_name='agent_name',
        test_id='some_test_id',
        agent_id_file='some_file',
    )
    cancellation = Cancellation()

    factory_mock = mock.Mock(spec=ClientFactory)
    transport_factory.return_value = factory_mock
    serve_single_job.return_value = JobResult(status='OK', exit_code=0)
    assert run_serve(config, cancellation, logging.getLogger(__name__)) == 0


@mock.patch.object(TransportFactory, 'get')
@mock.patch.object(UltaService, 'serve_single_job')
def test_run_serve_store_agent_id_fail(serve_single_job, transport_factory, fs_mock: FS):
    config = UltaConfig(
        command='',
        environment='DEFAULT',
        transport='mocked',
        backend_service_url='',
        iam_service_url='',
        logging_service_url='',
        object_storage_url='',
        work_dir='/tmp/test',
        lock_dir='/tmp/test',
        request_frequency=1,
        instance_lt_created=False,
        folder_id='some_folder_id',
        agent_name='agent_name',
        test_id='some_test_id',
        agent_id_file='some_file',
    )
    cancellation = Cancellation()

    factory_mock = mock.Mock(spec=ClientFactory)
    transport_factory.return_value = factory_mock
    serve_single_job.return_value = JobResult(status='OK', exit_code=0)
    f_mock = mock.mock_open()
    f_mock().write.side_effect = Exception('Unable to save file')
    assert run_serve(config, cancellation, logging.getLogger(__name__)) == 0
