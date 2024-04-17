import logging
import pytest
from pathlib import Path
from unittest.mock import patch
from ulta.common.file_system import FS, FileSystemObserver, parse_bytes, format_bytes
from ulta.common.cancellation import Cancellation
from ulta.common.state import State


MB = 2**20
GB = 2**30


@pytest.mark.parametrize(
    'value, expected',
    [
        ('1', 1),
        ('', -1),
        ('155aag', -1),
        ('15M', 15 * 2**20),
        ('18G', 18 * 2**30),
        ('6T', 6 * 2**40),
        ('129k', 129 * 2**10),
        ('129K', 129 * 2**10),
    ],
)
def test_parse_bytes(value, expected):
    assert parse_bytes(value) == expected


@pytest.mark.parametrize(
    'expected, value',
    [
        ('1', 1),
        ('15M', 15 * 2**20),
        ('18G', 18 * 2**30),
        ('6T', 6 * 2**40),
        ('129K', 129 * 2**10),
    ],
)
def test_format_bytes(value, expected):
    assert format_bytes(value) == expected


@pytest.mark.parametrize(
    'fs_usage_tool_result, expected_ok, expected_errors',
    [
        (
            '''Filesystem                         Size  Used Avail Use% Mounted on
/dev/mapper/ubuntu--vg-ubuntu--lv  461G  292G  146G  67% /
efivarfs                           383k  288k   91k  77% /sys/firmware/efi/efivars
/dev/nvme0n1p2                     2.1G  310M  1.7G  17% /boot
/dev/nvme0n1p1                     1.1G  6.4M  1.1G   1% /boot/efi
''',
            True,
            [],
        ),
        (
            '''Filesystem Size  Used Avail Use% Mounted on
/dev/sda1  100000000000  99000000000     1000000000  99% /
''',
            False,
            [
                'The error occured at "check free space /tmp": Agent has not enough free space for dir "/tmp": 953M; required minimum 2G',
                'The error occured at "check free space /other/tests": Agent has not enough free space for dir "/other/tests": 953M; required minimum 2G',
            ],
        ),
        (
            '''Filesystem Size  Used Avail  Use% Mounted on
/dev/sda1  100000000000  99000000000      1000000000   99% /tmp
/dev/sda2  100000000000  99000000000      1000000000   99% /var
/dev/sda3  100000000000  90000000000     10000000000  90% /
''',
            False,
            [
                'The error occured at "check free space /tmp": Agent has not enough free space for dir "/tmp": 953M; required minimum 2G'
            ],
        ),
        (
            '''Filesystem Size  Used Avail  Use% Mounted on
/dev/sda2  100G  95G     5G   95% /
/dev/sda3  100G  99G     1G   99% /other
''',
            False,
            [
                'The error occured at "check free space /other/tests": Agent has not enough free space for dir "/other/tests": 1G; required minimum 2G'
            ],
        ),
        (
            '''Filesystem Size  Used Avail  Use% Mounted on
/dev/sda1  100G  95G     5G   95% /
/dev/sda2  100G  99G    15M   99% /other
/dev/sda3  100G  97G     3G   97% /other/tests
''',
            True,
            [],
        ),
    ],
)
def test_healthcheck_free_space_with_df(fs_usage_tool_result, expected_ok, expected_errors):
    state = State()
    cancellation = Cancellation()
    fs_observer = FileSystemObserver(
        FS(tmp_dir=Path('/tmp'), tests_dir=Path('/other/tests'), lock_dir=Path('/var/lock')),
        state,
        logging.getLogger(),
        cancellation,
    )
    with patch('ulta.common.file_system.ensure_dir'):
        with patch.object(FileSystemObserver, '_get_fs_usage_native', side_effect=Exception('native not work')):
            with patch.object(FileSystemObserver, '_run_fs_usage_tool', return_value=fs_usage_tool_result):
                fs_observer.healthcheck()
                assert state.ok == expected_ok
                assert len(expected_errors) == len(state.current_errors())
                for e in expected_errors:
                    assert e in [ce.message for ce in state.current_errors()]


@pytest.mark.parametrize(
    'fs_usage_tool_result, expected_ok, expected_errors',
    [
        (
            {
                '/tmp': (461 * GB, 196 * GB, 196 * GB),
                '/other/tests': (461 * GB, 196 * GB, 196 * GB),
                '/var/lock': (461 * GB, 196 * GB, 196 * GB),
            },
            True,
            [],
        ),
        (
            {
                '/tmp': (100 * GB, 99 * GB, 1 * GB),
                '/other/tests': (100 * GB, 99 * GB, 1 * GB),
                '/var/lock': (100 * GB, 99 * GB, 1 * GB),
            },
            False,
            [
                'The error occured at "check free space /tmp": Agent has not enough free space for dir "/tmp": 1G; required minimum 2G',
                'The error occured at "check free space /other/tests": Agent has not enough free space for dir "/other/tests": 1G; required minimum 2G',
            ],
        ),
        (
            {
                '/tmp': (100 * GB, 99 * GB, 1 * GB),
                '/other/tests': (100 * GB, 90 * GB, 10 * GB),
                '/var/lock': (100 * GB, 99 * GB, 0 * MB),
            },
            False,
            [
                'The error occured at "check free space /tmp": Agent has not enough free space for dir "/tmp": 1G; required minimum 2G',
                'The error occured at "check free space /var/lock": Agent has not enough free space for dir "/var/lock": 0; required minimum 1M',
            ],
        ),
        (
            {
                '/tmp': (100 * GB, 95 * GB, 5 * GB),
                '/other/tests': (100 * GB, 99 * GB, 1 * GB),
                '/var/lock': (100 * GB, 95 * GB, 5 * GB),
            },
            False,
            [
                'The error occured at "check free space /other/tests": Agent has not enough free space for dir "/other/tests": 1G; required minimum 2G'
            ],
        ),
        (
            {
                '/tmp': (100 * GB, 95 * GB, 5 * GB),
                '/other/tests': (100 * GB, 97 * GB, 3 * GB),
                '/var/lock': (100 * GB, 95 * GB, 5 * GB),
            },
            True,
            [],
        ),
    ],
)
def test_healthcheck_free_space_native(fs_usage_tool_result, expected_ok, expected_errors):
    state = State()
    cancellation = Cancellation()
    fs_observer = FileSystemObserver(
        FS(tmp_dir=Path('/tmp'), tests_dir=Path('/other/tests'), lock_dir=Path('/var/lock')),
        state,
        logging.getLogger(),
        cancellation,
    )

    def os_statvfs_side_effect(path):
        return fs_usage_tool_result.get(path)

    with patch('ulta.common.file_system.ensure_dir'):
        with patch('shutil.disk_usage', side_effect=os_statvfs_side_effect):
            fs_observer.healthcheck()
            assert state.ok == expected_ok
            assert len(expected_errors) == len(state.current_errors())
            for e in expected_errors:
                assert e in [ce.message for ce in state.current_errors()]


def test_healthcheck_dir_access():
    state = State()
    cancellation = Cancellation()
    fs_observer = FileSystemObserver(
        FS(tmp_dir=Path('/tmp'), tests_dir=Path('/other/tests'), lock_dir=Path('/var/lock')),
        state,
        logging.getLogger(),
        cancellation,
    )

    def ensure_dir(path, *args, **kwargs):
        if str(path) == '/other/tests':
            raise PermissionError('Permission denied for /other/tests')
        return Path(path)

    ok_size = 3 * (2**30)
    fs_usage_tool_result = {
        '/tmp': FileSystemObserver._FSUsage(ok_size, 0, ok_size, '/tmp'),
        '/other/tests': FileSystemObserver._FSUsage(ok_size, 0, ok_size, '/other/tests'),
        '/var/lock': FileSystemObserver._FSUsage(ok_size, 0, ok_size, '/var/lock'),
    }

    with patch('ulta.common.file_system.ensure_dir', side_effect=ensure_dir):
        with patch.object(FileSystemObserver, '_get_fs_usage_native', return_value=fs_usage_tool_result):
            fs_observer.healthcheck()
            assert state.ok is False
            assert 1 == len(state.current_errors())
            assert (
                state.current_errors()[0].message
                == 'The error occured at "check working dir /other/tests": Permission denied for /other/tests'
            )
