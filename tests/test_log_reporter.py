import logging
import pytest
from queue import Queue
from unittest import mock
from ulta.common.agent import AgentInfo, AgentOrigin
from ulta.common.config import UltaConfig
from ulta.common.interfaces import ClientFactory
from ulta.common.logging import LogMessage
from ulta.common.reporter import NullReporter
from ulta.service.log_reporter import make_log_reporter, make_events_reporter


@pytest.fixture
def default_config():
    def _inner(**kwargs):
        return UltaConfig(
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
            **kwargs,
        )

    return _inner


def test_log_reporter_is_null_if_anonymous_agent(default_config):
    logger = logging.getLogger('some_logger')
    config = default_config(log_group_id='lgg1')
    agent = AgentInfo(id=None, name=None, version=None, origin=AgentOrigin.EXTERNAL, folder_id='some_folder_id')
    factory_mock = mock.Mock(spec=ClientFactory)
    client = mock.Mock()
    factory_mock.create_logging_client.return_value = client
    reporter = make_log_reporter(logger, config, agent, factory_mock, None)
    assert isinstance(reporter, NullReporter)


def test_log_reporter_is_null_if_no_log_group_id(default_config):
    logger = logging.getLogger('some_logger')
    config = default_config(log_group_id='')
    agent = AgentInfo(id='agent_id', name='name', version=None, origin=AgentOrigin.EXTERNAL, folder_id='some_folder_id')
    factory_mock = mock.Mock(spec=ClientFactory)
    client = mock.Mock()
    factory_mock.create_logging_client.return_value = client
    reporter = make_log_reporter(logger, config, agent, factory_mock, None)
    assert isinstance(reporter, NullReporter)


def test_log_reporter_smoke(default_config):
    logger = logging.getLogger('some_logger')
    config = default_config(log_group_id='lggg_11')
    agent = AgentInfo(id='idid', name='my-name', version=None, origin=AgentOrigin.EXTERNAL, folder_id='some_folder_id')
    factory_mock = mock.Mock(spec=ClientFactory)
    client = mock.Mock()
    factory_mock.create_logging_client.return_value = client
    reporter = make_log_reporter(logger, config, agent, factory_mock, None)

    assert not isinstance(reporter, NullReporter)

    logger.info('INFO_1 %(s)s', dict(s=1, v='okok'))
    logger.error('ERROR_2 %s', 'ss')
    reporter.report()

    calls = client.send_log.mock_calls
    logging.info('calls: %s', calls)
    assert len(calls) == 1

    call_kwargs = calls[0].kwargs

    assert call_kwargs['log_group_id'] == 'lggg_11'
    assert len(call_kwargs['log_data']) == 2

    log_data = call_kwargs['log_data'][0]
    assert isinstance(log_data, LogMessage)
    assert log_data.message == 'INFO_1 1'
    assert log_data.labels == {'agent_id': 'idid', 'agent_name': 'my-name', 's': '1', 'v': 'okok'}

    log_data = call_kwargs['log_data'][1]
    assert isinstance(log_data, LogMessage)
    assert log_data.message == 'ERROR_2 ss'
    assert log_data.labels == {'agent_id': 'idid', 'agent_name': 'my-name'}


def test_log_reporter_consumes_cached_events(default_config):
    logger = logging.getLogger('some_logger')
    config = default_config(log_group_id='lggg_11')
    agent = AgentInfo(id='idid', name='my-name', version=None, origin=AgentOrigin.EXTERNAL, folder_id='some_folder_id')
    factory_mock = mock.Mock(spec=ClientFactory)
    client = mock.Mock()
    factory_mock.create_logging_client.return_value = client
    cached = Queue()
    cached.put(
        logging.LogRecord(
            name='logger',
            level=logging.INFO,
            pathname='pp',
            lineno=1,
            msg='cached event 1',
            args=None,
            exc_info=None,
        )
    )
    cached.put(
        logging.LogRecord(
            name='logger',
            level=logging.INFO,
            pathname='pp',
            lineno=1,
            msg='cached event 2',
            args=[{'v': 'k'}],
            exc_info=None,
        ),
    )

    reporter = make_log_reporter(logger, config, agent, factory_mock, cached)

    assert not isinstance(reporter, NullReporter)

    reporter.report()

    calls = client.send_log.mock_calls
    logging.info('calls: %s', calls)
    assert len(calls) == 1

    call_kwargs = calls[0].kwargs
    assert call_kwargs['log_group_id'] == 'lggg_11'
    assert len(call_kwargs['log_data']) == 2

    log_data = call_kwargs['log_data'][0]
    assert isinstance(log_data, LogMessage)
    assert log_data.message == 'cached event 1'
    assert log_data.labels == {'agent_id': 'idid', 'agent_name': 'my-name'}

    log_data = call_kwargs['log_data'][1]
    assert isinstance(log_data, LogMessage)
    assert log_data.message == 'cached event 2'
    assert log_data.labels == {'agent_id': 'idid', 'agent_name': 'my-name', 'v': 'k'}


def test_loadtesting_log_reporter_smoke(default_config):
    logger = logging.getLogger('some_logger')
    config = default_config(log_group_id='lggg_11')
    agent = AgentInfo(
        id='idid', name='my-name', version='1.2.3', origin=AgentOrigin.EXTERNAL, folder_id='some_folder_id'
    )
    factory_mock = mock.Mock(spec=ClientFactory)
    client = mock.Mock()
    factory_mock.create_events_log_client.return_value = client
    reporter = make_events_reporter(logger, config, agent, factory_mock)

    assert not isinstance(reporter, NullReporter)

    logger.info('INFO_1 %(s)s', dict(s=1, v='okok'))
    logger.error('ERROR_2 %s', 'ss')
    reporter.report()

    calls = client.send_log.mock_calls
    logging.info('calls: %s', calls)

    assert len(calls) == 1
    call_kwargs = calls[0].kwargs
    assert len(call_kwargs['log_data']) == 2

    log_data = call_kwargs['log_data'][0]
    assert isinstance(log_data, LogMessage)
    assert log_data.message == 'INFO_1 1'
    assert log_data.labels == {
        'agent_id': 'idid',
        'agent_name': 'my-name',
        'agent_version': '1.2.3',
        's': '1',
        'v': 'okok',
    }

    log_data = call_kwargs['log_data'][1]
    assert isinstance(log_data, LogMessage)
    assert log_data.message == 'ERROR_2 ss'
    assert log_data.labels == {'agent_id': 'idid', 'agent_name': 'my-name', 'agent_version': '1.2.3'}

    # test limits

    long_message = ''.join(['1234567890'] * 205)
    attrs = {f'attr_{i}': f'v{i}' for i in range(70)}
    logger.info(long_message, attrs)

    attr_with_long_value = {'vv': ''.join(['1234567890'] * 20)}
    logger.info(long_message, attr_with_long_value)
    reporter.report()

    calls = client.send_log.mock_calls
    logging.info('calls: %s', calls)

    assert len(calls) == 2
    call_kwargs = calls[1].kwargs
    assert len(call_kwargs['log_data']) == 2

    log_data = call_kwargs['log_data'][0]
    assert isinstance(log_data, LogMessage)
    assert len(log_data.labels) == 64
    assert len(log_data.message) == 2000

    log_data = call_kwargs['log_data'][1]
    assert isinstance(log_data, LogMessage)
    assert 'vv' in log_data.labels
    assert len(log_data.labels.get('vv', '')) == 100
