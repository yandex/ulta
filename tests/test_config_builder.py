import os
import pytest
from collections import defaultdict
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
    proxy.request_frequency = 10
    proxy.no_cache = False

    assert target.get('agent_name') == 'some_name'
    assert target.get('folder_id') == 'some_folder'
    assert target.get('request_frequency') == 10
    assert target.get('no_cache') is False
    assert target.get('work_dir') is None
    assert target.get('service_account_key_path') is None

    assert history['agent_name'] == [(source, 'some_name')]
    assert history['folder_id'] == [(source, 'some_folder'), ('old source', 'old_folder')]
    assert history['request_frequency'] == [(source, 10)]
    assert history['no_cache'] == [(source, False)]
    assert len(history) == 4


def test_config_proxy_modified_only():
    source = 'test source'
    target = dict(
        work_dir='/some/path',
        request_frequency=5,
    )
    history = defaultdict(
        list,
        {
            'work_dir': [('old source', '/some/path')],
            'request_frequency': [('old source', 5)],
        },
    )
    proxy = _ConfigBuilder._ConfigProxy(source, history, target, modified_only=True)

    proxy.work_dir = '/some/path'
    proxy.folder_id = 'some_folder'
    proxy.request_frequency = 10
    proxy.no_cache = False

    assert target.get('work_dir') == '/some/path'
    assert target.get('folder_id') == 'some_folder'
    assert target.get('request_frequency') == 10
    assert target.get('no_cache') is False
    assert target.get('service_account_key_path') is None

    assert history['work_dir'] == [('old source', '/some/path')]
    assert history['folder_id'] == [(source, 'some_folder')]
    assert history['request_frequency'] == [(source, 10), ('old source', 5)]
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
    backend_service_url='loadtesting.somewhere.com:3320',
    command='GO',
    compute_instance_id='yc_compute_instance_xkdf',
    environment='CUSTOM_ENV',
    folder_id='yc_folder_155',
    iam_service_url='iam.domain.huh',
    iam_token='unique-auth-token',
    instance_lt_created=True,
    lock_dir='/var/locks/dirs',
    logging_level='VERYCUSTOM',
    logging_path='/var/logs/superlogs',
    logging_service_url='logging-ingester.ddd',
    netort_resource_manager='netortoverride',
    no_cache=True,
    oauth_token='the_token_oauth',
    object_storage_url='s3.amazon.maybe',
    plugins=['ulta.yc', 'my_custom_plugin.package'],
    request_frequency=16,
    service_account_id='asdfg',
    service_account_key_id='hkjl',
    service_account_key_path='mnopq',
    service_account_private_key='rst',
    test_id='uvwx',
    transport='yc',
    work_dir='~/.ulta',
    labels={'l1': 'v1', 'purpose': 'test'},
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
        )
    )

    assert_config(
        builder.build(),
        EXPECTED_CONFIG,
        [
            'agent_version',
            'compute_instance_id',
            'instance_lt_created',
            'request_frequency',
            'service_account_key_id',
            'service_account_private_key',
            'iam_token',
            'oauth_token',
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
            'LOADTESTING_LOG_PATH': '/var/logs/superlogs',
            'LOADTESTING_LOG_LEVEL': 'VERYCUSTOM',
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
            'request_frequency',
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
