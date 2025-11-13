import contextlib
import os
import stat
import time
import logging
import pytest
from pathlib import Path
from unittest.mock import patch
from ulta.common.config import UltaConfig
from ulta.common.job import Job
from ulta.common.file_system import (
    FS,
    FilesystemUsage,
    FilesystemCleanup,
    FileSystemObserver,
    parse_bytes,
    format_bytes,
    make_fs_from_ulta_config,
)
from ulta.common.cancellation import Cancellation
from ulta.common.state import State
from yandextank.contrib.netort.netort.resource import ResourceManager, ResourceManagerConfig

MB = 2**20
GB = 2**30


@contextlib.contextmanager
def none_mutable_lock(blocking: bool = False):
    yield False


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
                'The error occured at "check free space /tmp": Agent has not enough free space '
                'for dir "/tmp": 953M; required minimum 2G',
                'The error occured at "check free space /other/tests": Agent has not enough free space '
                'for dir "/other/tests": 953M; required minimum 2G',
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
                'The error occured at "check free space /tmp": Agent has not enough free space '
                'for dir "/tmp": 953M; required minimum 2G'
            ],
        ),
        (
            '''Filesystem Size  Used Avail  Use% Mounted on
/dev/sda2  100G  95G     5G   95% /
/dev/sda3  100G  99G     1G   99% /other
''',
            False,
            [
                'The error occured at "check free space /other/tests": Agent has not enough free space '
                'for dir "/other/tests": 1G; required minimum 2G'
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
        none_mutable_lock,
        logging.getLogger(),
        cancellation,
    )
    with patch('ulta.common.file_system.ensure_dir'):
        with patch.object(FilesystemUsage, '_get_fs_usage_native', side_effect=Exception('native not work')):
            with patch.object(FilesystemUsage, '_run_fs_usage_tool', return_value=fs_usage_tool_result):
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
                'The error occured at "check free space /tmp": Agent has not enough free space '
                'for dir "/tmp": 1G; required minimum 2G',
                'The error occured at "check free space /other/tests": Agent has not enough free space '
                'for dir "/other/tests": 1G; required minimum 2G',
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
                'The error occured at "check free space /tmp": Agent has not enough free space '
                'for dir "/tmp": 1G; required minimum 2G',
                'The error occured at "check free space /var/lock": Agent has not enough free space '
                'for dir "/var/lock": 0; required minimum 1M',
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
                'The error occured at "check free space /other/tests": Agent has not enough free space '
                'for dir "/other/tests": 1G; required minimum 2G'
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
        none_mutable_lock,
        logging.getLogger(),
        cancellation,
    )

    def os_statvfs_side_effect(path):
        return fs_usage_tool_result.get(path.as_posix())

    with patch('ulta.common.file_system.ensure_dir'):
        with patch('shutil.disk_usage', side_effect=os_statvfs_side_effect):
            with (
                patch.object(Path, 'rglob', new=lambda p, pattern: []),
                patch.object(Path, 'iterdir', new=lambda p: []),
            ):
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
        none_mutable_lock,
        logging.getLogger(),
        cancellation,
    )

    def ensure_dir(path, *args, **kwargs):
        if str(path) == '/other/tests':
            raise PermissionError('Permission denied for /other/tests')
        return Path(path)

    ok_size = 3 * (2**30)
    fs_usage_tool_result = {
        Path('/tmp'): FilesystemUsage.FSUsage(ok_size, 0, ok_size, Path('/tmp')),
        Path('/other/tests'): FilesystemUsage.FSUsage(ok_size, 0, ok_size, Path('/other/tests')),
        Path('/var/lock'): FilesystemUsage.FSUsage(ok_size, 0, ok_size, Path('/var/lock')),
    }

    with patch('ulta.common.file_system.ensure_dir', side_effect=ensure_dir):
        with patch.object(FilesystemUsage, '_get_fs_usage_native', return_value=fs_usage_tool_result):
            fs_observer.healthcheck()
            assert state.ok is False
            assert 1 == len(state.current_errors())
            assert (
                state.current_errors()[0].message
                == 'The error occured at "check working dir /other/tests": Permission denied for /other/tests'
            )


def test_healthcheck_skip_cleanup_on_mutation_lock():
    state = State()
    cancellation = Cancellation()
    fs = FS(tmp_dir=Path('/tmp'), tests_dir=Path('/other/tests'), lock_dir=Path('/var/lock'))
    fs_observer = FileSystemObserver(
        fs,
        state,
        none_mutable_lock,
        logging.getLogger(),
        cancellation,
    )

    def ensure_dir(path, *args, **kwargs):
        return Path(path)

    ok_size = 3 * (2**30)
    fs_usage_tool_result = {
        fs.tmp_dir: FilesystemUsage.FSUsage(ok_size, ok_size, 0, fs.tmp_dir),
        fs.tests_dir: FilesystemUsage.FSUsage(ok_size, ok_size, 0, fs.tests_dir),
        fs.lock_dir: FilesystemUsage.FSUsage(ok_size, 0, ok_size, fs.lock_dir),
    }

    with patch('ulta.common.file_system.ensure_dir', side_effect=ensure_dir):
        with patch.object(FilesystemUsage, '_get_fs_usage_native', return_value=fs_usage_tool_result):
            with (
                patch.object(FilesystemCleanup, 'clean_tests_dirs') as patch_clean_tests_dirs,
                patch.object(FilesystemCleanup, 'clean_temporary_dir') as patch_clean_temporary_dir,
            ):
                fs_observer.healthcheck()
                assert state.ok is False
                assert 2 == len(state.current_errors())
                assert 'Agent has not enough free space for dir' in state.current_errors()[0].message
                assert patch_clean_tests_dirs.assert_called
                assert patch_clean_temporary_dir.assert_called


@pytest.mark.parametrize('work_dir', ('/tmp/ulta_work', 'ulta_work'))
@pytest.mark.parametrize('lock_dir', ('/tmp/ulta_lock', 'ulta_lock'))
@pytest.mark.parametrize('netort_dir', ('/tmp/netort_cache', 'netort_cache'))
def test_filesystem_cleanup(work_dir, lock_dir, netort_dir):
    fs = make_fs_from_ulta_config(
        UltaConfig(
            work_dir=work_dir,
            lock_dir=lock_dir,
            command='',
            environment='',
            transport='',
            backend_service_url='',
            iam_service_url='',
            logging_service_url='',
            object_storage_url='',
            request_interval=0,
            instance_lt_created=False,
        )
    )
    netort_dir = Path(netort_dir)

    old_job = Job(
        id='old_id',
        config={},
        test_data_dir=(fs.tmp_dir / 'old_id').absolute().as_posix(),
        artifact_dir_path=(fs.tests_dir / 'old_id').absolute().as_posix(),
    )
    job = Job(
        id='id',
        config={},
        test_data_dir=(fs.tmp_dir / 'id').absolute().as_posix(),
        artifact_dir_path=(fs.tests_dir / 'id').absolute().as_posix(),
    )

    now = time.time()
    ftimes_now = (now, now, now)
    ftimes_old = (now - 86400 * 10, now - 86400 * 10, now - 86400 * 10)
    FILES = {
        # st_mode, st_ino, st_dev, st_nlink, st_uid, st_gid, st_size, st_atime, st_mtime, st_ctime
        fs.tmp_dir: os.stat_result((stat.S_IFDIR, 0, 0, 0, 0, 0, 0, *ftimes_now)),
        fs.tmp_dir / old_job.id: os.stat_result((stat.S_IFDIR, 0, 0, 0, 0, 0, 0, *ftimes_old)),
        fs.tmp_dir / old_job.id / 'ammo.gz': os.stat_result((stat.S_IFREG, 0, 0, 0, 0, 0, 0, *ftimes_old)),
        fs.tmp_dir / old_job.id / 'config.json': os.stat_result((stat.S_IFREG, 0, 0, 0, 0, 0, 0, *ftimes_old)),
        fs.tmp_dir / job.id: os.stat_result((stat.S_IFDIR, 0, 0, 0, 0, 0, 0, *ftimes_now)),
        fs.tmp_dir / job.id / 'ammo.gz': os.stat_result((stat.S_IFREG, 0, 0, 0, 0, 0, 0, *ftimes_now)),
        fs.tmp_dir / job.id / 'config.json': os.stat_result((stat.S_IFREG, 0, 0, 0, 0, 0, 0, *ftimes_now)),
        fs.tests_dir: os.stat_result((stat.S_IFDIR, 0, 0, 0, 0, 0, 0, *ftimes_now)),
        fs.tests_dir / old_job.id: os.stat_result((stat.S_IFDIR, 0, 0, 0, 0, 0, 0, *ftimes_old)),
        fs.tests_dir / old_job.id / 'ammo': os.stat_result((stat.S_IFREG, 0, 0, 0, 0, 0, 0, *ftimes_old)),
        fs.tests_dir / old_job.id / 'config.yaml': os.stat_result((stat.S_IFREG, 0, 0, 0, 0, 0, 0, *ftimes_old)),
        fs.tests_dir / job.id: os.stat_result((stat.S_IFDIR, 0, 0, 0, 0, 0, 0, *ftimes_now)),
        fs.tests_dir / job.id / 'ammo': os.stat_result((stat.S_IFREG, 0, 0, 0, 0, 0, 0, *ftimes_now)),
        fs.tests_dir / job.id / 'config.yaml': os.stat_result((stat.S_IFREG, 0, 0, 0, 0, 0, 0, *ftimes_now)),
        fs.tests_dir / 'stpd-cache': os.stat_result((stat.S_IFDIR, 0, 0, 0, 0, 0, 0, *ftimes_now)),
        fs.tests_dir / 'stpd-cache' / 'file': os.stat_result((stat.S_IFREG, 0, 0, 0, 0, 0, 1024, *ftimes_now)),
        fs.tests_dir / 'lunapark': os.stat_result((stat.S_IFDIR, 0, 0, 0, 0, 0, 0, *ftimes_now)),
        fs.tests_dir / 'lunapark' / '123456': os.stat_result((stat.S_IFLNK, 0, 0, 0, 0, 0, 0, *ftimes_now)),
        netort_dir: os.stat_result((stat.S_IFDIR, 0, 0, 0, 0, 0, 0, *ftimes_now)),
        netort_dir / 'http_file': os.stat_result((stat.S_IFREG, 0, 0, 0, 0, 0, 1024, *ftimes_now)),
        netort_dir / 's3_file': os.stat_result((stat.S_IFREG, 0, 0, 0, 0, 0, 1024, *ftimes_now)),
    }
    FILES.update({k.resolve(): v for k, v in FILES.items()})
    EXPECTED_RMTREE = {fs.tests_dir / 'old_id', fs.tmp_dir / 'old_id'}
    EXPECTED_UNLINK = {fs.tests_dir / 'stpd-cache/file', netort_dir / 'http_file', netort_dir / 's3_file'}

    unlinked_files = set()

    rm_cf = ResourceManagerConfig()
    rm_cf.tmp_path = netort_dir
    rm = ResourceManager(rm_cf)
    with patch.object(FilesystemUsage, 'get', return_value=FilesystemUsage.FSUsage(0, 0, 0, Path())):
        with patch('shutil.rmtree') as patch_rmtree:
            with (
                patch.object(Path, 'unlink', new=lambda p: unlinked_files.add(p)),
                patch.object(Path, 'rglob', new=lambda p, pattern: [f for f in FILES if f.is_relative_to(p)]),
                patch.object(Path, 'iterdir', new=lambda p: [f for f in FILES if f.parent == p]),
                patch.object(Path, 'stat', new=lambda p, follow_symlinks=False: FILES[p]),
                patch.object(Path, 'exists', new=lambda p: p in FILES),
            ):
                FilesystemCleanup(logging.getLogger(), fs, job, rm).cleanup()

                assert set(arg[0][0] for arg in patch_rmtree.call_args_list) == EXPECTED_RMTREE
                assert unlinked_files == EXPECTED_UNLINK
