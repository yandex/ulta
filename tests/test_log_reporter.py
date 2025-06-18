import logging
import pytest
from functools import reduce
from queue import Queue
from unittest import mock
from ulta.common.agent import AgentInfo, AgentOrigin
from ulta.common.config import UltaConfig
from ulta.common.interfaces import ClientFactory, LogMessage
from ulta.common.reporter import NullReporter
from ulta.service.log_reporter import (
    make_log_reporter,
    LogMessageProcessor,
    LabelsKey,
    with_extra,
)
from ulta.service.service_context import LabelContext


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
            report_log_events=True,
            report_yandextank_log_events_level='DEBUG',
            log_max_chunk_size=1000,
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
    assert not isinstance(reporter, NullReporter)
    assert len(reporter._handlers) == 1


def test_log_reporter_smoke(default_config):
    logger = logging.getLogger('some_logger')

    config = default_config(log_group_id='lggg_11')
    agent = AgentInfo(id='idid', name='my-name', version=None, origin=AgentOrigin.EXTERNAL, folder_id='some_folder_id')
    factory_mock = mock.Mock(spec=ClientFactory)
    client = mock.Mock()
    factory_mock.create_logging_client.return_value = client
    context = LabelContext()
    reporter = make_log_reporter(logger, config, agent, factory_mock, context)
    assert not isinstance(reporter, NullReporter)
    with context.agent(agent):
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
    assert log_data.labels == {'agent_id': 'idid', 'agent_name': 'my-name', 'agent_version': '', 's': '1', 'v': 'okok'}

    log_data = call_kwargs['log_data'][1]
    assert isinstance(log_data, LogMessage)
    assert log_data.message == 'ERROR_2 ss'
    assert log_data.labels == {'agent_id': 'idid', 'agent_name': 'my-name', 'agent_version': ''}


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

    context = LabelContext()
    with context.agent(agent):
        reporter = make_log_reporter(logger, config, agent, factory_mock, context, cached)
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
    assert log_data.labels == {
        'agent_id': 'idid',
        'agent_name': 'my-name',
        'agent_version': '',
    }

    log_data = call_kwargs['log_data'][1]
    assert isinstance(log_data, LogMessage)
    assert log_data.message == 'cached event 2'
    assert log_data.labels == {'agent_id': 'idid', 'agent_name': 'my-name', 'agent_version': '', 'v': 'k'}


def test_loadtesting_log_reporter_smoke(default_config):
    logger = logging.getLogger('some_logger')

    config = default_config(log_group_id='lggg_11')
    agent = AgentInfo(
        id='idid', name='my-name', version='1.2.3', origin=AgentOrigin.EXTERNAL, folder_id='some_folder_id'
    )
    factory_mock = mock.Mock(spec=ClientFactory)
    client = mock.Mock()
    factory_mock.create_events_log_client.return_value = client
    context = LabelContext()

    reporter = make_log_reporter(logger, config, agent, factory_mock, context)
    assert not isinstance(reporter, NullReporter)

    with context.agent(agent):
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


def test_loadtesting_log_reporter_with_limits(default_config):
    logger = logging.getLogger('some_logger')

    config = default_config(log_group_id='lggg_11')
    agent = AgentInfo(
        id='idid', name='my-name', version='1.2.3', origin=AgentOrigin.EXTERNAL, folder_id='some_folder_id'
    )
    factory_mock = mock.Mock(spec=ClientFactory)
    client = mock.Mock()
    factory_mock.create_events_log_client.return_value = client

    context = LabelContext()
    reporter = make_log_reporter(logger, config, agent, factory_mock, context)
    assert not isinstance(reporter, NullReporter)

    with context.agent(agent):
        long_message = '1234567890' * 205
        attrs = {f'attr_{i}': f'v{i}' for i in range(2000)}
        logger.info(long_message, attrs)

        attr_with_long_value = {'vv': '1234567890' * 2000}
        logger.info(long_message, attr_with_long_value)

    reporter.report()

    calls = client.send_log.mock_calls
    logging.info('calls: %s', calls)

    assert len(calls) == 1
    call_kwargs = calls[0].kwargs
    assert len(call_kwargs['log_data']) == 2

    log_data = call_kwargs['log_data'][0]
    assert isinstance(log_data, LogMessage)
    assert _get_labels_len(log_data.labels) <= 8192
    assert len(log_data.message) == 2000
    assert log_data.labels == {'agent_id': 'idid', 'agent_name': 'my-name', 'agent_version': '1.2.3'} | {
        f'attr_{i}': f'v{i}' for i in range(697)
    }

    log_data = call_kwargs['log_data'][1]
    assert isinstance(log_data, LogMessage)
    assert _get_labels_len(log_data.labels) == 8192
    assert len(log_data.message) == 2000
    assert log_data.labels == {
        'agent_id': 'idid',
        'agent_name': 'my-name',
        'agent_version': '1.2.3',
        'vv': '1234567890' * 814 + '...',
    }


@pytest.mark.parametrize(
    'args, expected_size',
    [
        (('123', 555), 6),
        (('some_key with space', 'wow, value'), 29),
        (('some_key with space', None), 19),
    ],
)
def test_loadtesting_log_reporter_args_pair_size(args, expected_size):
    assert LogMessageProcessor._get_args_pair_size(args) == expected_size


@pytest.mark.parametrize(
    'msg, args, expected_message, expected_labels, expected_labels_len',
    [
        ('short_message', None, 'short_message', {}, 0),
        ('1234567890' * 10, None, '1234567890123456789012345...9012345678901234567890', {}, 0),
        (
            '%s %s %s %s',
            (
                '11',
                '22',
                'asdfbf',
                None,
            ),
            '11 22 asdfbf None',
            {},
            0,
        ),
        ('', ({'v1': 150, 'a1': 'asdfbbb'},), '', {'v1': '150', 'a1': 'asdfbbb'}, 14),
        (
            '%(v1)s',
            ({'v1': '12345' * 20},),
            '1234512345123451234512345...4512345123451234512345',
            {'v1': '1234512345123451234512345...'},
            30,
        ),
    ],
)
def test_loadtesting_log_reporter_prepare(msg, args, expected_message, expected_labels, expected_labels_len):
    log_reporter = LogMessageProcessor(
        'lgg1',
        'abcdf',
        mock.Mock(),
        lambda *args: None,
        max_message_length=50,
        max_labels_size=64,
    )
    r = logging.LogRecord(
        name='logger_stdout',
        level=logging.WARNING,
        pathname='/some/path/name',
        lineno=130,
        msg=msg,
        args=args,
        exc_info=None,
    )
    r = with_extra(r, {LabelsKey.CONTEXT_LABELS_KEY: {'agent_id': 'abcdf', 'some_other_value': '10500'}})
    actual_msg = log_reporter.prepare_log_record(r)

    expected_labels = {'agent_id': 'abcdf', 'some_other_value': '10500'} | expected_labels
    assert actual_msg.message == expected_message
    assert actual_msg.level == logging.WARNING
    assert actual_msg.labels == expected_labels

    assert _get_labels_len(actual_msg.labels) == 34 + expected_labels_len


def test_message_log_processor_prepare_expected_labels():
    log_reporter = LogMessageProcessor(
        'lgg1',
        'abcdf',
        mock.Mock(),
        lambda *args: None,
        max_message_length=50,
        max_labels_size=128,
    )
    r = logging.LogRecord(
        name='logger_stdout',
        level=logging.WARNING,
        pathname='/some/path/name',
        lineno=130,
        msg='text',
        args=None,
        exc_info=None,
    )
    r = with_extra(
        r,
        {
            LabelsKey.CONTEXT_LABELS_KEY: {'agent_id': 'abcdf', 'some_other_value': '10500'},
            LabelsKey.TYPE_KEY: 'request/response',
            LabelsKey.SOURCE_KEY: 'some_source',
            LabelsKey.FILEPATH_KEY: '/var/lib/ulta/file.txt',
        },
    )
    actual_msg = log_reporter.prepare_log_record(r)

    expected_labels = {
        'agent_id': 'abcdf',
        'some_other_value': '10500',
        'type': 'request/response',
        'source': 'some_source',
        'filepath': '/var/lib/ulta/file.txt',
    }

    assert actual_msg.labels == expected_labels


@pytest.mark.parametrize(
    'report_yandextank_request_response_events',
    [
        (True,),
        (False,),
    ],
)
def test_log_message_processor_report_yandextank_request_response_events(report_yandextank_request_response_events):
    client = mock.Mock()
    log_processor = LogMessageProcessor(
        'lgg1',
        'abcdf',
        client,
        lambda *args: None,
        max_message_length=50,
        max_labels_size=64,
        report_request_response_events=report_yandextank_request_response_events,
    )
    r = logging.LogRecord(
        name='logger_stdout',
        level=logging.WARNING,
        pathname='/some/path/name',
        lineno=130,
        msg='some text',
        args=None,
        exc_info=None,
    )
    r = with_extra(r, {'type': 'sample'})

    log_processor.handle('request_id', [r])
    if report_yandextank_request_response_events:
        client.send_log.assert_called_once()
    else:
        client.send_log.assert_not_called()


def _get_labels_len(labels: dict[str, str]):
    return reduce(lambda acc, p: acc + len(p[0]) + len(p[1]), labels.items(), 0)
