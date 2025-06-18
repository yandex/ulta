import argparse
import sys
from strenum import StrEnum
from dataclasses import dataclass


class Command(StrEnum):
    SERVE = 'serve'
    RUN = 'run'
    VERSION = 'version'


@dataclass
class CliArgs:
    command: str | None = None
    config_file_path: str | None = None
    no_cache: bool | None = False
    work_dir: str | None = None
    lock_dir: str | None = None
    plugins: list[str] | None = None
    netort_resource_manager: str | None = None

    backend_service_url: str | None = None
    iam_service_url: str | None = None
    logging_service_url: str | None = None
    object_storage_url: str | None = None

    environment: str | None = None
    transport: str | None = None
    folder_id: str | None = None
    agent_name: str | None = None
    agent_id_file: str | None = None
    service_account_id: str | None = None
    service_account_key_path: str | None = None
    log_path: str | None = None
    log_level: str | None = None
    labels: dict[str, str] | None = None

    test_id: str | None = None
    log_group_id: str | None = None
    no_report_log_events: bool | None = False
    report_yandextank_log_events_level: str | None = None
    report_yandextank_request_response_events: bool | None = False


def parse_cli_args() -> CliArgs:
    return parse_args(sys.argv[1:])


def parse_args(args: list[str]) -> CliArgs:
    parser = _create_parser()
    return CliArgs(**vars(parser.parse_args(args)))


def _create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        usage='ulta [options] command [command-options]\n'
        + 'ulta --folder-id=<yc-folder-id> --agent-name=<agent-name> --sa-key-file=/path/to/service_account/key serve\n'
        + 'ulta --config=/path/to/config/file\n'
        + 'ulta --folder-id=<yc-folder-id> --sa-key-file=/path/to/service_account/key run <test-id-to-run>',
        description='Run loadtesting agent. See how to use external agents in loadtesting service:\n'
        + '      https://cloud.yandex.com/en/docs/load-testing/tutorials/loadtesting-external-agent\n\n'
        + 'Following commands are supported:\n'
        + '  serve - run agent as a service (persistent agent). Auto-register itself in loadtesting service and poll tests from backend\n'
        + '      ex: ulta --agent-name=my-persistent-agent --folder-id=b1gabcdefghijk --sa-key-file=/run/sa_key.json serve\n'
        + '  run   - execute single test and shutdown\n'
        + '      ex: ulta --folder-id=b1gabcdefghijk --sa-key-file=/run/sa_key.json run ff9abcdefghijk',
    )

    parser.add_argument('--folder-id', dest='folder_id', help='Id of folder in Yandex Cloud with Loadtesting service')
    parser.add_argument('--agent-name', dest='agent_name', help='Persistent agent unique name')
    parser.add_argument('--agent-id-file', dest='agent_id_file', help='Path to file, where agent id should be written')
    parser.add_argument('--no-cache', dest='no_cache', action='store_true', help="Don't use cached agent id.")
    parser.add_argument('--work-dir', dest='work_dir', help="Path to ulta working directory.")
    parser.add_argument('--lock-dir', dest='lock_dir', help="Path to yandextank lock directory.")
    parser.add_argument('--netort-rm', dest='netort_resource_manager', help='Resource manager module to use')
    parser.add_argument('--backend-url', dest='backend_service_url', help='Loadtesting backend service URL')
    parser.add_argument('--iam-url', dest='iam_service_url', help='IAM service URL')
    parser.add_argument('--logging-url', dest='logging_service_url', help='Logging service URL')
    parser.add_argument('--storage-url', dest='object_storage_url', help='Object storage URL')
    parser.add_argument('--remote-log-storage', dest='log_group_id', help='ID of YC Cloud Logging log group')
    parser.add_argument(
        '--no-report-log-events',
        dest='no_report_log_events',
        action='store_true',
        help='Disable reporting log events to loadtesting backend service.',
    )
    parser.add_argument(
        '--report-yandextank-log-events-level',
        dest='report_yandextank_log_events_level',
        help='Report yandextank log events of specified level to loadtesting backend service: CRITICAL, ERROR, WARNING, INFO, DEBUG, DISABLED. Default: INFO',
    )
    parser.add_argument(
        '--report-yandextank-request-response-events',
        dest='report_yandextank_request_response_events',
        action='store_true',
        help='Upload sampled requests/responses to Load Testing / Cloud Logging if those are supplied by load generator and log events reporting is enabled',
    )

    parser.add_argument(
        '--sa-id',
        dest='service_account_id',
        help='Id of service account in Yandex Cloud that will be used to request authentication JWT token',
    )
    parser.add_argument(
        '--sa-key-file',
        dest='service_account_key_path',
        help='Path to service account private key for authorization in Loadtesting service. \nSee https://cloud.yandex.com/en-ru/docs/iam/operations/authorized-key/create for details',
    )
    parser.add_argument('-c', '--config', dest='config_file_path', help='path to agent config file')
    parser.add_argument(
        '--environment',
        dest='environment',
        help='Turn on features available in runtime environment\n'
        + '  DEFAULT - run with no extra features (default value)\n'
        + '  YANDEX_CLOUD_COMPUTE - run in YC Compute runtime, allows to use Compute Metadata',
    )
    parser.add_argument(
        '--transport',
        dest='transport',
        help='Use custom package as factory for backend client. Default: ulta.yc',
    )
    parser.add_argument(
        '--log-level',
        dest='log_level',
        help='Log level: CRITICAL, ERROR, WARNING, INFO, DEBUG. Default: INFO',
    )
    parser.add_argument(
        '--log-path',
        dest='log_path',
        help='Log file path, or set empty for logging to stdout.',
    )
    parser.add_argument(
        '--labels',
        dest='labels',
        action=StoreKeyValueAction,
        help='Agent labels when run as external agent in format --labels key=value[,key=value...]',
    )
    parser.add_argument(
        '--plugins',
        dest='plugins',
        action=StoreListAction,
        help='Which plugins to use while config building in format --plugins plugin[,plugin...]',
    )

    parser.add_argument(
        'command',
        nargs='?',
        default=Command.SERVE,
        choices=[Command.SERVE, Command.RUN, Command.VERSION],
        help='\n'.join(
            [
                'command to run. available options:',
                '  serve   - (default) run loadtesting agent as a persistent agent service'
                '  run     - run on-demand agent and execute single test'
                '  version - show current version',
            ]
        ),
    )
    # TODO: CLOUDLOAD-678 - create and run test
    run_command = parser.add_argument_group('run command args')
    # run_command_args.add_argument('-c', '--test-config-path', dest='test_config_path')
    run_command.add_argument('test_id', help='id of test to execute; usage: ulta run <test_id>', nargs='?')

    return parser


class StoreKeyValueAction(argparse.Action):
    def __call__(self, parser, namespace, values: str, option_string=None):
        try:
            res = parse_str_as_key_values(values)
        except ValueError as e:
            raise argparse.ArgumentError(self, e.args[0])
        setattr(namespace, self.dest, res)


class StoreListAction(argparse.Action):
    def __call__(self, parser, namespace, values: str, option_string=None):
        try:
            res = parse_str_as_list_values(values)
        except ValueError as e:
            raise argparse.ArgumentError(self, e.args[0])
        setattr(namespace, self.dest, res)


def parse_str_as_key_values(s: str | None) -> dict[str, str] | None:
    if s is None:
        return None
    res = {}
    for pair in s.split(','):
        pair = pair.strip()
        if len(pair) == 0:
            continue
        keyvalue = pair.split('=', maxsplit=1)
        if len(keyvalue) == 1:
            raise ValueError('Expected argument in format key=value[,key=value]...')
        res[keyvalue[0]] = keyvalue[1]
    return res


def parse_str_as_list_values(s: str | None) -> list[str] | None:
    if s is None:
        return None
    res = []
    for value in s.split(','):
        value = value.strip()
        if value:
            res.append(value)
    return res
