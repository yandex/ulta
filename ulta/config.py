import os
import yaml
from collections import defaultdict
from tabulate import tabulate
from typing import Any
from pathlib import Path
from contextlib import contextmanager
from ulta.cli_args import CliArgs, Command, parse_str_as_key_values, parse_str_as_list_values, parse_cli_args
from ulta.common.config import UltaConfig, DEFAULT_ENVIRONMENT, ExternalConfigLoader
from ulta.common.utils import normalize_path, get_and_convert, str_to_timedelta
from ulta.module import load_plugins
from ulta.version import VERSION

CONFIG_PATH_ENV = 'LOADTESTING_AGENT_CONFIG'
RUN_IN_ENVIRONMENT_ENV = 'LOADTESTING_ENVIRONMENT'
METADATA_AGENT_VERSION_ATTR = 'agent-version'
CONFIG_PATHS = [
    '/var/lib/ulta/config.yaml',
    normalize_path('~/.ulta/config.yaml'),
    normalize_path('~/.config/ulta.yaml'),
]


class _ConfigBuilder:
    class _ConfigProxy(UltaConfig):
        def __init__(self, source: str, history: dict[str, list], config: dict, modified_only: bool = False):
            self._history = history
            self._config = config
            self._current_source = source
            self._modified_only = modified_only

        def __getattr__(self, name):
            if self._is_field(name):
                return self._config.get(name)
            return object.__getattribute__(self, name)

        def __setattr__(self, name, value: Any):
            if not self._is_field(name):
                object.__setattr__(self, name, value)
                return
            if not self._current_source:
                raise AttributeError('UltaConfig must be configured via calling builder("source") method')
            if value is not None and (not self._modified_only or value != self._config.get(name)):
                history_value = '***sensitive_data***' if self._is_sensitive(name) else value
                self._history[name] = [(self._current_source, history_value)] + self._history[name]
                self._config[name] = value

        def _is_field(self, name):
            return UltaConfig.model_fields.get(name) is not None

        def _is_sensitive(self, name):
            if not self._is_field(name):
                return False

            f = UltaConfig.model_fields.get(name)
            return f.json_schema_extra is not None and f.json_schema_extra.get('sensitive') is True

    def __init__(self):
        self.config = {}
        self._history = defaultdict(list)

    @contextmanager
    def next_source(self, source: str, modified_only: bool = False):
        yield _ConfigBuilder._ConfigProxy(source, self._history, self.config, modified_only=modified_only)

    def explain(self) -> str:
        explanation = []
        for name, history in self._history.items():
            line = [name] + [f'<- "{h_item[1]}" [{h_item[0]}]' for h_item in history]
            explanation.append(line)
        return '\n' + tabulate(explanation)

    def default_config(self):
        with self.next_source('default') as config:
            config.command = Command.SERVE
            config.environment = DEFAULT_ENVIRONMENT
            config.plugins = ['ulta.yc']
            config.transport = 'ulta.yc'
            config.work_dir = normalize_path('~/.ulta')
            config.lock_dir = '/var/lock'
            config.backend_service_url = 'loadtesting.api.cloud.yandex.net:443'
            config.iam_service_url = 'iam.api.cloud.yandex.net:443'
            config.logging_service_url = 'ingester.logging.yandexcloud.net:443'
            config.object_storage_url = 'https://storage.yandexcloud.net'
            config.request_interval = 1
            config.reporter_interval = 10
            config.log_level = 'INFO'
            config.agent_version = VERSION
            config.no_cache = False
            config.instance_lt_created = False

    def load_file_config(self, config_path: str):
        config_path = normalize_path(config_path)
        if not os.path.exists(config_path):
            return

        content = yaml.safe_load(Path(config_path).read_text('utf-8'))
        with self.next_source(config_path) as config:
            config.environment = content.get('environment')
            config.transport = content.get('transport_factory')
            config.netort_resource_manager = content.get('netort_resource_manager')
            config.backend_service_url = content.get('load_testing_url')
            config.logging_service_url = content.get('logging_url')
            config.iam_service_url = content.get('iam_token_service_url')
            config.object_storage_url = content.get('object_storage_url')
            config.agent_id_file = content.get('agent_id_file')
            config.work_dir = content.get('client_workdir')
            config.lock_dir = content.get('lock_dir')
            config.log_max_chunk_size = get_and_convert(content.get('log_max_chunk_size'), int)
            config.log_retention_period = get_and_convert(content.get('log_retention_period'), str_to_timedelta)
            config.log_group_id = content.get('log_group_id')
            config.log_path = content.get('log_path', content.get('logging_path'))
            config.log_level = content.get('log_level', content.get('logging_level'))
            config.request_interval = get_and_convert(
                content.get('request_interval', content.get('request_frequency')), int
            )
            config.reporter_interval = get_and_convert(content.get('reporter_interval'), int)
            config.agent_name = content.get('agent_name')
            config.folder_id = content.get('folder_id')
            config.service_account_id = content.get('service_account_id')
            config.service_account_key_id = content.get('key_id')
            config.service_account_key_path = content.get('private_key')
            config.service_account_private_key = content.get('private_key_payload')
            config.labels = content.get('labels')
            config.plugins = content.get('plugins')
            config.no_cache = content.get('no_cache')

    def load_env_config(self):
        with self.next_source('env') as config:
            config.environment = os.getenv(RUN_IN_ENVIRONMENT_ENV, config.environment)
            config.transport = os.getenv('LOADTESTING_TRANSPORT_FACTORY')
            config.backend_service_url = os.getenv('LOADTESTING_BACKEND_SERVICE_URL')
            config.iam_service_url = os.getenv('LOADTESTING_IAM_SERVICE_URL')
            config.object_storage_url = os.getenv('LOADTESTING_OBJECT_STORAGE_URL')
            config.logging_service_url = os.getenv('LOADTESTING_LOGGING_SERVICE_URL')
            config.agent_id_file = os.getenv('LOADTESTING_AGENT_ID_FILE')
            config.work_dir = os.getenv('WORK_DIR')
            config.lock_dir = os.getenv('LOCK_DIR')
            config.log_group_id = os.getenv('LOADTESTING_LOG_REMOTE_STORAGE')
            config.log_path = os.getenv('LOADTESTING_LOG_PATH')
            config.log_level = os.getenv('LOADTESTING_LOG_LEVEL')
            config.agent_name = os.getenv('LOADTESTING_AGENT_NAME')
            config.folder_id = os.getenv('LOADTESTING_FOLDER_ID')
            config.service_account_key_path = os.getenv('LOADTESTING_SA_KEY_FILE')
            config.iam_token = os.getenv('LOADTESTING_YC_TOKEN')
            config.oauth_token = os.getenv('LOADTESTING_OAUTH_TOKEN')
            config.test_id = os.getenv('TEST_ID')
            # support LOADTESTING_SA_ID, LOADTESTING_SA_KEY_ID, LOADTESTING_SA_KEY_PAYLOAD only for backward compatibility:
            # https://cloud.yandex.com/en-ru/docs/load-testing/tutorials/loadtesting-external-agent
            # remove them after fix docs
            config.service_account_id = os.getenv('LOADTESTING_SA_ID')
            config.service_account_key_id = os.getenv('LOADTESTING_SA_KEY_ID')
            config.service_account_private_key = os.getenv('LOADTESTING_SA_KEY_PAYLOAD')
            config.netort_resource_manager = os.getenv('RESOURCE_MANAGER_OVERRIDE')
            config.labels = parse_str_as_key_values(os.getenv('LOADTESTING_LABELS', ''))
            config.plugins = parse_str_as_list_values(os.getenv('LOADTESTING_PLUGINS'))
            if os.getenv('LOADTESTING_NO_CACHE'):
                config.no_cache = True

    def load_args_config(self, args: CliArgs):
        if not args:
            return

        with self.next_source('args') as config:
            config.command = args.command
            if config.command in ['', None]:
                config.command = Command.SERVE
            config.backend_service_url = args.backend_service_url
            config.iam_service_url = args.iam_service_url
            config.logging_service_url = args.logging_service_url
            config.object_storage_url = args.object_storage_url
            config.agent_name = args.agent_name
            config.agent_id_file = args.agent_id_file
            config.folder_id = args.folder_id
            config.environment = args.environment
            config.transport = args.transport
            config.service_account_id = args.service_account_id
            config.service_account_key_path = args.service_account_key_path
            config.log_path = args.log_path
            config.log_level = args.log_level
            config.test_id = args.test_id
            config.work_dir = args.work_dir
            config.lock_dir = args.lock_dir
            config.labels = args.labels
            config.netort_resource_manager = args.netort_resource_manager
            config.plugins = args.plugins
            config.log_group_id = args.log_group_id

            if args.no_cache:
                config.no_cache = args.no_cache

    def resolve_paths(self):
        with self.next_source('resolve_paths', modified_only=True) as config:
            config.agent_id_file = normalize_path(config.agent_id_file or os.path.join(config.work_dir, 'agentid'))
            config.work_dir = normalize_path(config.work_dir)
            config.lock_dir = normalize_path(config.lock_dir)
            config.service_account_key_path = normalize_path(config.service_account_key_path)
            config.log_path = normalize_path(config.log_path)

    def build(self) -> UltaConfig:
        return UltaConfig(**self.config)


def configure() -> tuple[UltaConfig, str]:
    args = parse_cli_args()

    builder = _ConfigBuilder()
    builder.default_config()
    builder.load_file_config(get_config_file_path(args))
    builder.load_env_config()

    cfg = builder.build()
    if cfg.plugins:
        load_plugins(cfg.plugins)
        for c in detect_env_config_plugins():
            if c is not None and c.should_apply(builder.config.get('environment')):
                config_loader = c.create()
                with builder.next_source(config_loader.name()) as config:
                    config_loader(config)

    builder.load_args_config(args)
    builder.resolve_paths()

    return builder.build(), builder.explain()


def get_config_file_path(args: CliArgs | None = None) -> str:
    if not args:
        args = CliArgs()

    if args.config_file_path:
        return args.config_file_path

    if config_path := os.getenv(CONFIG_PATH_ENV):
        return config_path

    for p in CONFIG_PATHS:
        if os.path.isfile(config_path := normalize_path(p)):
            return config_path

    return ''


def detect_env_config_plugins() -> list[type[ExternalConfigLoader]]:
    return ExternalConfigLoader.__subclasses__()
