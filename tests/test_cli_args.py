import pytest
from ulta.cli_args import parse_args, CliArgs
from ulta.yc.config import YANDEX_COMPUTE


@pytest.mark.parametrize(('command', 'expected_command'), [(None, 'serve'), ('serve', 'serve'), ('run', 'run')])
@pytest.mark.parametrize(
    ('args', 'expected'),
    [
        ([], CliArgs()),
        (['--environment=YANDEX_CLOUD_COMPUTE'], CliArgs(environment=YANDEX_COMPUTE)),
        (['--agent-name=some_agent', '--folder-id=asdf'], CliArgs(folder_id='asdf', agent_name='some_agent')),
        (['--config=/my/path'], CliArgs(config_file_path='/my/path')),
        (['-c', '/my/path'], CliArgs(config_file_path='/my/path')),
        (['--config='], CliArgs(config_file_path='')),
    ],
)
def test_common_cli_args(args, command, expected_command, expected: CliArgs):
    if command:
        args = args + [command]
    expected.command = expected_command
    assert parse_args(args) == expected


@pytest.mark.parametrize(
    ('args', 'expected'),
    [
        (['--agent-name', 'blah', 'run', 'haha'], CliArgs(agent_name='blah', test_id='haha')),
        (['run', 'haha'], CliArgs(test_id='haha')),
    ],
)
def test_run_cli_args(args, expected: CliArgs):
    expected.command = 'run'
    assert parse_args(args) == expected
