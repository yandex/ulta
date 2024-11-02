from ulta.service.command import run_service, register_loadtesting_agent
from ulta.service.service import UltaService
from ulta.service.service_context import LabelContext
from ulta.service.tank_client import TankClient
from ulta.common.agent import AgentInfo, AgentOrigin
from ulta.common.config import UltaConfig
from ulta.common.cancellation import Cancellation
from ulta.common.file_system import FS
from ulta.common.job import JobResult
from ulta.common.interfaces import ClientFactory, TransportFactory
from ulta.common.state import GenericObserver, State

import logging
from unittest import mock


@mock.patch.object(TransportFactory, 'get')
@mock.patch.object(UltaService, 'serve_single_job')
def test_run_service_smoke(serve_single_job, transport_factory, fs_mock: FS):
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
        request_interval=1,
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
    logger = logging.getLogger(__name__)
    agent = AgentInfo(
        id='agent_id', name='agent_name', version=None, origin=AgentOrigin.EXTERNAL, folder_id='some_folder_id'
    )
    tank_client = TankClient(
        logger=logger,
        fs=fs_mock,
        loadtesting_client=transport_factory.create_job_data_uploader_client(agent),
        data_uploader_api_address=config.backend_service_url,
    )
    assert (
        run_service(
            config=config,
            cancellation=cancellation,
            service_state=State(),
            transport_factory=transport_factory,
            agent=agent,
            fs=fs_mock,
            logger=logger,
            label_context=LabelContext(),
            tank_client=tank_client,
        )
        == 0
    )


@mock.patch.object(TransportFactory, 'get')
@mock.patch.object(UltaService, 'serve_single_job')
def test_store_agent_id_fail(serve_single_job, transport_factory, fs_mock: FS):
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
        request_interval=1,
        instance_lt_created=False,
        folder_id='some_folder_id',
        agent_name='agent_name',
        test_id='some_test_id',
        agent_id_file='some_file',
        agent_version='5.5',
    )
    cancellation = Cancellation()

    factory_mock = mock.Mock(spec=ClientFactory)
    transport_factory.return_value = factory_mock
    serve_single_job.return_value = JobResult(status='OK', exit_code=0)
    f_mock = mock.mock_open()
    f_mock().write.side_effect = Exception('Unable to save file')
    observer = GenericObserver(State(), logging.getLogger(__name__), cancellation)
    agent = register_loadtesting_agent(
        config, transport_factory.create_agent_client(), observer, logging.getLogger(__name__)
    )
    assert agent.name == 'agent_name'
    assert agent.folder_id == 'some_folder_id'
    assert agent.version == '5.5'
    assert agent.origin == AgentOrigin.EXTERNAL
