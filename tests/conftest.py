import pytest
import logging
import sys
import traceback

from pathlib import Path
from unittest.mock import patch, mock_open
from yandextank.core.tankcore import JobsStorage

from ulta.common.agent import AgentInfo
from ulta.common.ammo import Ammo
from ulta.common.file_system import FS, FilesystemUsage
from ulta.service.service import UltaService
from ulta.service.status_reporter import StatusReporter
from ulta.service.tank_client import TankClient
from ulta.service.log_uploader_service import LogUploaderService
from ulta.service.artifact_uploader import S3ArtifactUploader, ArtifactCollector
from ulta.yc.backend_client import YCLoadtestingClient
from ulta.yc.s3_client import YCS3Client


@pytest.fixture()
def patch_cwd(request, monkeypatch):
    if not request.path.as_posix().endswith('tests'):
        monkeypatch.chdir(request.fspath.dirname)


@pytest.fixture()
def patch_loadtesting_agent_is_external_anonymous_agent():
    with patch.object(AgentInfo, 'is_anonymous_external_agent') as p:
        yield p


@pytest.fixture()
def patch_loadtesting_client_claim_tank_status():
    with patch.object(YCLoadtestingClient, 'claim_tank_status') as p:
        yield p


@pytest.fixture()
def patch_loadtesting_client_claim_job_status():
    with patch.object(YCLoadtestingClient, 'claim_job_status') as p:
        p.return_value = 0
        yield p


@pytest.fixture()
def patch_loadtesting_client_get_job():
    with patch.object(YCLoadtestingClient, 'get_job') as p:
        yield p


@pytest.fixture()
def patch_loadtesting_client_get_job_signal():
    with patch.object(YCLoadtestingClient, 'get_job_signal') as p:
        yield p


@pytest.fixture()
def patch_loadtesting_s3_client_download():
    with patch.object(YCS3Client, 'download') as p:
        yield p


@pytest.fixture()
def patch_loadtesting_client_download_transient_ammo():
    with patch.object(YCLoadtestingClient, 'download_transient_ammo') as p:
        yield p


@pytest.fixture()
def patch_tank_client_run_job():
    with patch.object(TankClient, 'run_job') as p:
        yield p


@pytest.fixture()
def patch_tank_client_prepare_job():
    with patch.object(TankClient, 'prepare_job') as p:
        p.return_value = {'success': True, 'id': 'MockedTankId'}
        yield p


@pytest.fixture()
def patch_dump_config_file():
    with patch.object(TankClient, 'dump_job_config') as p:
        p.return_value = '/tmp/some_path'
        yield p


@pytest.fixture()
def patch_tank_client_get_tank_status():
    with patch.object(TankClient, 'get_tank_status') as p:
        yield p


@pytest.fixture()
def patch_tank_client_get_job_status():
    with patch.object(TankClient, 'get_job_status') as p:
        yield p


@pytest.fixture()
def patch_tank_client_stop_job():
    with patch.object(TankClient, 'stop_job') as p:
        yield p


@pytest.fixture()
def patch_tank_client_finish():
    with patch.object(TankClient, 'finish') as p:
        yield p


@pytest.fixture()
def patch_tank_client_cleanup():
    with patch.object(TankClient, 'cleanup') as p:
        yield p


@pytest.fixture(autouse=True)
def patch_push_job():
    with patch.object(JobsStorage, 'push_job') as p:
        yield p


@pytest.fixture(autouse=True)
def patch_create_storage_file():
    with patch.object(JobsStorage, '_create_storage_file') as p:
        yield p


@pytest.fixture(autouse=True)
def patch_jobs_storage_get_cloud_job_id():
    with patch.object(JobsStorage, 'get_cloud_job_id') as p:
        p.return_value = 'cloud_job_id'
        yield p


@pytest.fixture()
def patch_job_fetcher_extract_ammo():
    with patch.object(UltaService, '_extract_ammo') as p:
        p.return_value = [Ammo('ammo', 'ammo')]
        yield p


@pytest.fixture()
def patch_ulta_serve_lt_job():
    with patch.object(UltaService, 'serve_lt_job') as p:
        yield p


@pytest.fixture()
def patch_ulta_serve_lt_signal():
    with patch.object(UltaService, 'serve_lt_signal') as p:
        yield p


@pytest.fixture()
def patch_status_reporter_report_tank_status():
    with patch.object(StatusReporter, 'report_tank_status') as p:
        yield p


@pytest.fixture()
def patch_log_uploader_send_logs():
    with patch.object(LogUploaderService, '_send_log') as p:
        yield p


@pytest.fixture()
def patch_s3_uploader_collect_artifacts():
    with patch.object(ArtifactCollector, 'collect_artifacts') as p:
        yield p


@pytest.fixture()
def patch_s3_uploader_upload_artifacts():
    with patch.object(S3ArtifactUploader, '_upload_artifacts') as p:
        yield p


@pytest.fixture()
def fs_mock():
    with patch.object(FilesystemUsage, '_get_fs_usage_native', return_value={Path('/tmp'): None}):
        with patch('builtins.open', mock_open()):
            with patch('os.mkdir'):
                yield FS(
                    tmp_dir=Path('/tmp'),
                    tests_dir=Path('/tmp'),
                    lock_dir=Path('/tmp'),
                )


@pytest.fixture()
def check_threads_leak():
    threads_before_test = set()
    for thread_ident, frame in sys._current_frames().items():
        threads_before_test.add(thread_ident)

    yield

    while True:
        extra_frames = []
        for thread_ident, frame in sys._current_frames().items():
            if thread_ident not in threads_before_test:
                extra_frames.append(frame)
        if not extra_frames:
            return
        # logging just for case test ends by ya make timeout
        logging.warn(f'Test generate {len(extra_frames)} extra threads. Waiting...')
        logging.info('Extra thread trace: ')
        logging.info(''.join(traceback.format_stack(extra_frames[0], limit=50)))
        raise AssertionError(f'Test generate extra {len(extra_frames)} threads.')
