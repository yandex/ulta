import argparse
import sys
from strenum import StrEnum
from dataclasses import dataclass
from typing import Optional, List


class Command(StrEnum):
    SERVE = 'serve'
    RUN = 'run'
    VERSION = 'version'


@dataclass
class CliArgs:
    command: Optional[str] = None
    config_file_path: Optional[str] = None
    no_cache: Optional[bool] = False
    work_dir: Optional[str] = None
    lock_dir: Optional[str] = None

    environment: Optional[str] = None
    transport: Optional[str] = None
    folder_id: Optional[str] = None
    agent_name: Optional[str] = None
    service_account_key_path: Optional[str] = None
    log_path: Optional[str] = None
    log_level: Optional[str] = None
    labels: Optional[dict] = None

    test_id: Optional[str] = None


def parse_cli_args() -> CliArgs:
    return parse_args(sys.argv[1:])


def parse_args(args: List[str]) -> CliArgs:
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
    parser.add_argument('--no-cache', dest='no_cache', action='store_true', help="Don't use cached agent id.")
    parser.add_argument('--work-dir', dest='work_dir', help="Path to ulta working directory.")
    parser.add_argument('--lock-dir', dest='lock_dir', help="Path to yandextank lock directory.")

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


def parse_str_as_key_values(s: Optional[str]) -> Optional[dict]:
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


def parse_str_as_list_values(s: Optional[str]) -> Optional[list]:
    if s is None:
        return None
    res = []
    for value in s.split(','):
        value = value.strip()
        if value:
            res.append(value)
    return res
