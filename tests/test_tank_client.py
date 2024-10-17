import logging
import os
import stat
import time
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from ulta.service.tank_client import TankClient, FilesystemCleanup
from ulta.common.config import UltaConfig
from ulta.common.file_system import FS, make_fs_from_ulta_config
from ulta.common.job import Job, JobPluginType
from ulta.common.job_status import AdditionalJobStatus
from yandextank.contrib.netort.netort.resource import ResourceManager, ResourceManagerConfig


@pytest.mark.parametrize(
    ('response', 'exp_error', 'exp_error_type'),
    [
        ({}, '', None),
        ({'error': 'some error'}, 'some error', None),
        ({'error': 'some error', 'tank_msg': 'some tank_msg'}, 'some error', None),
        ({'tank_msg': 'some tank_msg'}, 'some tank_msg', 'internal'),
        ({'tank_msg': 'some tank_msg', 'exit_code': 1}, 'some tank_msg', 'internal'),
        ({'error': 'some error', 'exit_code': 1}, 'some error', None),
        ({'exit_code': 1}, 'Unknown error', None),
        ({'exit_code': 0}, '', None),
    ],
)
def test_extract_error(response, exp_error, exp_error_type):
    error, error_type = TankClient.extract_error(response)
    assert error == exp_error
    assert error_type == exp_error_type


@pytest.mark.parametrize(
    ('job_response', 'exp_status', 'exp_exit_code'),
    [
        ({}, AdditionalJobStatus.FAILED, 1),
        ({'status_code': 'FINISHED', 'exit_code': 21}, AdditionalJobStatus.AUTOSTOPPED, 21),
        ({'status_code': 'FINISHED', 'exit_code': 28}, AdditionalJobStatus.AUTOSTOPPED, 28),
        ({'status_code': 'FINISHED'}, 'FINISHED', 0),
        ({'status_code': 'TESTING'}, 'TESTING', None),
    ],
)
def test_parse_job_status(
    job_response,
    exp_status,
    exp_exit_code,
):
    status = TankClient.parse_job_status(job_response)
    assert status.status == exp_status
    assert status.exit_code == exp_exit_code


def test_finish_awaits_running_jobs(fs_mock: FS):
    client = TankClient(logging.getLogger(), fs_mock, MagicMock(), 'api_address')
    w1, w2, f1, f2 = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    client._background_workers = [w1, w2]
    client._finalizers = [f1, f2]
    client.finish()
    w1.finish.assert_called()
    w2.finish.assert_called()
    f1.run.assert_called()
    f2.run.assert_called()


@pytest.mark.parametrize(
    ('config', 'expected_patch'),
    [
        (
            {'uploader': {'enabled': True, 'package': JobPluginType.UPLOADER, 'api_address': 'api_address'}},
            {'uploader': {'enabled': False}},
        ),
        (
            {'uploader': {'enabled': False, 'package': JobPluginType.UPLOADER, 'api_address': 'api_address'}},
            {},
        ),
        (
            {
                'first_uploader': {'enabled': True, 'package': JobPluginType.UPLOADER, 'api_address': 'api_address'},
                'overload_uploader': {
                    'enabled': True,
                    'package': JobPluginType.UPLOADER,
                    'api_address': 'other_address',
                },
                'some_other_uploader': {
                    'enabled': True,
                    'package': JobPluginType.UPLOADER,
                    'api_address': 'third_address',
                },
                'autostop': {'enabled': True, 'package': JobPluginType.AUTOSTOP, 'api_address': 'third_address'},
            },
            {'first_uploader': {'enabled': False}},
        ),
    ],
)
def test_disable_uploaders(config, expected_patch, fs_mock: FS):
    tank_client = TankClient(logging.getLogger(), fs_mock, MagicMock(), 'api_address')
    job = Job(id='id', config=config)
    patch = tank_client._generate_disable_data_uploaders_patch(job)
    assert expected_patch == patch


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
        netort_dir / 'http': os.stat_result((stat.S_IFDIR, 0, 0, 0, 0, 0, 0, *ftimes_now)),
        netort_dir / 'http' / 'file': os.stat_result((stat.S_IFREG, 0, 0, 0, 0, 0, 1024, *ftimes_now)),
        netort_dir / 's3': os.stat_result((stat.S_IFDIR, 0, 0, 0, 0, 0, 0, *ftimes_now)),
        netort_dir / 's3' / 'file': os.stat_result((stat.S_IFREG, 0, 0, 0, 0, 0, 1024, *ftimes_now)),
    }
    FILES.update({k.resolve(): v for k, v in FILES.items()})
    EXPECTED_RMTREE = {fs.tests_dir / 'old_id', fs.tmp_dir / 'old_id'}
    EXPECTED_UNLINK = {fs.tests_dir / 'stpd-cache/file', netort_dir / 'http' / 'file', netort_dir / 's3' / 'file'}

    unlinked_files = set()

    rm_cf = ResourceManagerConfig()
    rm_cf.tmp_path = netort_dir
    rm = ResourceManager(rm_cf)
    with patch.object(FilesystemCleanup, '_get_free_space', return_value=0):
        with patch('shutil.rmtree') as patch_rmtree:
            with (
                patch.object(Path, 'unlink', new=lambda p: unlinked_files.add(p)),
                patch.object(Path, 'rglob', new=lambda p, pattern: [f for f in FILES if f.is_relative_to(p)]),
                patch.object(Path, 'iterdir', new=lambda p: [f for f in FILES if f.parent == p]),
                patch.object(Path, 'stat', new=lambda p: FILES[p]),
                patch.object(Path, 'exists', new=lambda p: p in FILES),
            ):
                FilesystemCleanup(logging.getLogger(), fs, job, rm).cleanup()

                assert set(arg[0][0] for arg in patch_rmtree.call_args_list) == EXPECTED_RMTREE
                assert unlinked_files == EXPECTED_UNLINK
