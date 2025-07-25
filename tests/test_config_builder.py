import os
import pytest
from collections import defaultdict
from datetime import timedelta
from pydantic import ValidationError
from ulta.cli_args import CliArgs
from ulta.common.config import UltaConfig
from ulta.config import _ConfigBuilder
from unittest.mock import patch


def test_config_proxy():
    source = 'test source'
    target = {}
    history = defaultdict(
        list,
        {
            'folder_id': [('old source', 'old_folder')],
        },
    )
    proxy = _ConfigBuilder._ConfigProxy(source, history, target)

    proxy.agent_name = 'some_name'
    proxy.folder_id = 'some_folder'
    proxy.request_interval = 10
    proxy.no_cache = False

    assert target.get('agent_name') == 'some_name'
    assert target.get('folder_id') == 'some_folder'
    assert target.get('request_interval') == 10
    assert target.get('no_cache') is False
    assert target.get('work_dir') is None
    assert target.get('service_account_key_path') is None

    assert history['agent_name'] == [(source, 'some_name')]
    assert history['folder_id'] == [(source, 'some_folder'), ('old source', 'old_folder')]
    assert history['request_interval'] == [(source, 10)]
    assert history['no_cache'] == [(source, False)]
    assert len(history) == 4


def test_config_proxy_modified_only():
    source = 'test source'
    target = dict(
        work_dir='/some/path',
        request_interval=5,
    )
    history = defaultdict(
        list,
        {
            'work_dir': [('old source', '/some/path')],
            'request_interval': [('old source', 5)],
        },
    )
    proxy = _ConfigBuilder._ConfigProxy(source, history, target, modified_only=True)

    proxy.work_dir = '/some/path'
    proxy.folder_id = 'some_folder'
    proxy.request_interval = 10
    proxy.no_cache = False

    assert target.get('work_dir') == '/some/path'
    assert target.get('folder_id') == 'some_folder'
    assert target.get('request_interval') == 10
    assert target.get('no_cache') is False
    assert target.get('service_account_key_path') is None

    assert history['work_dir'] == [('old source', '/some/path')]
    assert history['folder_id'] == [(source, 'some_folder')]
    assert history['request_interval'] == [(source, 10), ('old source', 5)]
    assert history['no_cache'] == [(source, False)]
    assert len(history) == 4


def assert_config(actual: UltaConfig, expected: UltaConfig, exclude_fields: list):
    # using this method to enforce 100% coverage of config fields
    invalid_fields = []
    assert_fields = set(expected.__dict__.keys()) - set(exclude_fields)
    for field in assert_fields:
        if getattr(actual, field) != getattr(expected, field):
            invalid_fields.append(f'{field}: expected "{getattr(expected, field)}", got "{getattr(actual, field)}"')
        elif getattr(actual, field) is None:
            invalid_fields.append(f'{field}: is None; please cover this field in test_load_*_config tests')

    assert not invalid_fields, 'Config assertion failed:\n{}'.format("\n".join(invalid_fields))


EXPECTED_CONFIG = UltaConfig(
    agent_id_file='path/to/agent_id_file',
    agent_name='ulta-agent',
    agent_version='15.2.11',
    state_api_port=6055,
    backend_service_url='loadtesting.somewhere.com:3320',
    reporter_interval=11,
    command='GO',
    compute_instance_id='yc_compute_instance_xkdf',
    environment='CUSTOM_ENV',
    folder_id='yc_folder_155',
    iam_service_url='iam.domain.huh',
    iam_token='unique-auth-token',
    instance_lt_created=True,
    lock_dir='/var/locks/dirs',
    log_group_id='sheesh_very_unique_group',
    log_level='VERYCUSTOM',
    log_path='/var/logs/superlogs',
    log_max_chunk_size=156,
    log_max_unsent_queue_size=30000,
    log_retention_period=timedelta(minutes=23, seconds=11),
    logging_service_url='logging-ingester.ddd',
    netort_resource_manager='netortoverride',
    no_cache=True,
    oauth_token='the_token_oauth',
    object_storage_url='s3.amazon.maybe',
    plugins=['ulta.yc', 'my_custom_plugin.package'],
    request_interval=16,
    service_account_id='asdfg',
    service_account_key_id='hkjl',
    service_account_key_path='mnopq',
    service_account_private_key='rst',
    test_id='uvwx',
    transport='yc',
    work_dir='~/.ulta',
    labels={'l1': 'v1', 'purpose': 'test'},
    aws_access_key_id='aws_access_key_id_12345',
    aws_secret_access_key='aws_access_key_id_secretsecret',
    report_log_events=True,
    report_yandextank_log_events_level='DEBUG',
    report_yandextank_request_response_events=True,
)


@pytest.mark.usefixtures('patch_cwd')
def test_load_file_config():
    builder = _ConfigBuilder()
    builder.default_config()
    builder.load_file_config('test_config/config.yaml')

    assert_config(
        builder.build(),
        EXPECTED_CONFIG,
        [
            'agent_version',
            'command',
            'compute_instance_id',
            'instance_lt_created',
            'test_id',
            'iam_token',
            'oauth_token',
            'custom_stdout_log_handler_factory',
        ],
    )


def test_load_args_config():
    builder = _ConfigBuilder()
    builder.default_config()
    builder.load_args_config(
        CliArgs(
            agent_name='ulta-agent',
            agent_id_file='path/to/agent_id_file',
            command='GO',
            environment='CUSTOM_ENV',
            folder_id='yc_folder_155',
            lock_dir='/var/locks/dirs',
            log_level='VERYCUSTOM',
            log_path='/var/logs/superlogs',
            log_group_id='sheesh_very_unique_group',
            no_cache=True,
            service_account_id='asdfg',
            service_account_key_path='mnopq',
            test_id='uvwx',
            transport='yc',
            work_dir='~/.ulta',
            labels={'l1': 'v1', 'purpose': 'test'},
            netort_resource_manager='netortoverride',
            plugins=['ulta.yc', 'my_custom_plugin.package'],
            backend_service_url='loadtesting.somewhere.com:3320',
            iam_service_url='iam.domain.huh',
            logging_service_url='logging-ingester.ddd',
            object_storage_url='s3.amazon.maybe',
            no_report_log_events=False,
            report_yandextank_log_events_level='DEBUG',
            report_yandextank_request_response_events=True,
        )
    )

    assert_config(
        builder.build(),
        EXPECTED_CONFIG,
        [
            'agent_version',
            'state_api_port',
            'compute_instance_id',
            'instance_lt_created',
            'request_interval',
            'reporter_interval',
            'service_account_key_id',
            'service_account_private_key',
            'iam_token',
            'oauth_token',
            'log_retention_period',
            'log_max_chunk_size',
            'log_max_unsent_queue_size',
            'aws_access_key_id',
            'aws_secret_access_key',
            'custom_stdout_log_handler_factory',
        ],
    )


def test_load_env_config():
    builder = _ConfigBuilder()
    builder.default_config()
    with patch.dict(
        os.environ,
        {
            'LOADTESTING_ENVIRONMENT': 'CUSTOM_ENV',
            'LOADTESTING_TRANSPORT_FACTORY': 'yc',
            'LOADTESTING_BACKEND_SERVICE_URL': 'loadtesting.somewhere.com:3320',
            'LOADTESTING_IAM_SERVICE_URL': 'iam.domain.huh',
            'LOADTESTING_LOGGING_SERVICE_URL': 'logging-ingester.ddd',
            'LOADTESTING_OBJECT_STORAGE_URL': 's3.amazon.maybe',
            'LOADTESTING_AGENT_ID_FILE': 'path/to/agent_id_file',
            'WORK_DIR': '~/.ulta',
            'LOCK_DIR': '/var/locks/dirs',
            'ULTA_SERVE_STATE_API_PORT': '6055',
            'LOADTESTING_LOG_REMOTE_STORAGE': 'sheesh_very_unique_group',
            'LOADTESTING_LOG_PATH': '/var/logs/superlogs',
            'LOADTESTING_LOG_LEVEL': 'VERYCUSTOM',
            'LOADTESTING_REPORT_LOG_EVENTS': 'YES',
            'LOADTESTING_REPORT_YANDEXTANK_LOG_EVENTS_LEVEL': 'DEBUG',
            'LOADTESTING_REPORT_YANDEXTANK_REQUEST_RESPONSE_EVENTS': 'YES',
            'LOADTESTING_AGENT_NAME': 'ulta-agent',
            'LOADTESTING_FOLDER_ID': 'yc_folder_155',
            'LOADTESTING_SA_KEY_FILE': 'mnopq',
            'LOADTESTING_YC_TOKEN': 'unique-auth-token',
            'LOADTESTING_OAUTH_TOKEN': 'the_token_oauth',
            'TEST_ID': 'uvwx',
            'LOADTESTING_SA_ID': 'asdfg',
            'LOADTESTING_SA_KEY_ID': 'hkjl',
            'LOADTESTING_SA_KEY_PAYLOAD': 'rst',
            'LOADTESTING_NO_CACHE': '1',
            'RESOURCE_MANAGER_OVERRIDE': 'netortoverride',
            'LOADTESTING_LABELS': 'l1=v1,purpose=test',
            'LOADTESTING_PLUGINS': 'ulta.yc,my_custom_plugin.package',
            'AWS_ACCESS_KEY_ID': 'aws_access_key_id_12345',
            'AWS_SECRET_ACCESS_KEY': 'aws_access_key_id_secretsecret',
        },
    ):
        builder.load_env_config()

    assert_config(
        builder.build(),
        EXPECTED_CONFIG,
        [
            'command',
            'agent_version',
            'compute_instance_id',
            'instance_lt_created',
            'request_interval',
            'reporter_interval',
            'log_retention_period',
            'log_max_chunk_size',
            'log_max_unsent_queue_size',
            'custom_stdout_log_handler_factory',
        ],
    )


@pytest.mark.parametrize(
    'labels',
    [
        {'-name': 'v'},
        {'name-ok': 'value-ok', '-name-not-ok': 'value'},
        {'invalid@char': 'v'},
        {'UPPERKEY': 'v'},
        {'': 'v'},
        {'asdfghijklasdfghijklasdfghijklasdfghijklasdfghijklasdfghijklasdfghijkl': 'more-than-64-key'},
        {'more-than-64-value': 'asdfghijklasdfghijklasdfghijklasdfghijklasdfghijklasdfghijklasdfghijkl'},
    ],
)
def test_validate_labels(labels):
    with pytest.raises(ValidationError):
        builder = _ConfigBuilder()
        builder.default_config()
        with builder.next_source('test') as c:
            c.labels = labels
        builder.resolve_paths()
        builder.build()


@pytest.mark.parametrize(
    'labels',
    [
        {'name': 'v'},
        {'name-ok': 'value-ok', 'name-ok2': 'value2'},
        {'somekey': 'v'},
        {'keywith1numbers2': 'v'},
    ],
)
def test_validate_labels_pass(labels):
    builder = _ConfigBuilder()
    builder.default_config()
    with builder.next_source('test') as c:
        c.labels = labels
    builder.resolve_paths()
    builder.build()
