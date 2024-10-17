import logging
import os
import shutil
import time
from pathlib import Path
from typing import Callable

from yandextank.contrib.netort.netort.resource import ResourceManager

from ulta.common.file_system import FS
from ulta.common.job import Job, JobPluginType


DEFAULT_STPD_CACHE_TTL = 7 * 86400
DEFAULT_NETORT_CACHE_TTL = 7 * 86400


class FilesystemCleanup:
    def __init__(
        self,
        logger: logging.Logger,
        fs: FS,
        job: Job,
        resource_manager: ResourceManager | None,
        stpd_cache_ttl=DEFAULT_STPD_CACHE_TTL,
        netort_cache_ttl=DEFAULT_NETORT_CACHE_TTL,
    ):
        self._logger = logger.getChild('FilesystemCleanup')
        self._fs = fs
        self._job = job
        self._resource_manager = resource_manager
        self._job_disk_limit = self._get_job_disk_limit()
        self._stpd_cache_dir = self._fs.tests_dir / 'stpd-cache'
        self._stpd_cache_ttl = stpd_cache_ttl
        self._netort_cache_ttl = netort_cache_ttl

        self._forbiden_dirs: set[Path] = {
            self._stpd_cache_dir,
            self._fs.tests_dir,
            self._fs.tmp_dir,
            self._fs.tests_dir / 'lunapark',
        }
        if self._job.test_data_dir:
            self._forbiden_dirs.add(Path(self._job.test_data_dir))
            self._forbiden_dirs.add(Path(self._job.test_data_dir.replace('/test_data_', '/')))
        if self._job.artifact_dir_path:
            self._forbiden_dirs.add(Path(self._job.artifact_dir_path))
        self._forbiden_dirs = {d.resolve() for d in self._forbiden_dirs if d.exists()}

    @staticmethod
    def _log_errors(function: Callable[['FilesystemCleanup'], None]) -> Callable[['FilesystemCleanup'], None]:
        def inner(self: 'FilesystemCleanup'):
            try:
                function(self)
            except Exception as e:
                self._logger.error('error during cleanup %(func)s: {ex}', {'func': function.__name__, 'ex': str(e)})

        return inner

    def _get_job_disk_limit(self) -> int:
        if rc := self._job.get_plugins(JobPluginType.RESOURCE_CHECK):
            job_disk_limit = int(rc[0][1].get('disk_limit', 2048))
        else:
            job_disk_limit = 2048
        if job_disk_limit <= 0:
            job_disk_limit = 2048
        # extra 100M bytes for possible miscalculations of ResourceCheck plugin
        # due to different approach to free space evaluation
        job_disk_limit += 100
        job_disk_limit *= 1024 * 1024
        return job_disk_limit

    def _get_free_space(self, path: Path) -> int:
        stat = os.statvfs(path)
        return stat.f_bavail * stat.f_frsize

    def _log_free_space(self, stage: str, path: Path):
        self._logger.debug(
            'free space %(stage)s cleanup %(path)s: %(free_space)s',
            dict(stage=stage, path=path, free_space=self._get_free_space(path)),
        )

    def _is_forbiden_dir(self, path: Path) -> bool:
        return path.resolve() in self._forbiden_dirs

    @_log_errors
    def _cleanup_temporary_dir(self):
        if not self._fs.tmp_dir.exists():
            self._logger.debug('temporary folder is not found')
            return

        self._log_free_space('before', self._fs.tmp_dir)
        tmp_objects = tuple(f for f in self._fs.tmp_dir.iterdir() if not self._is_forbiden_dir(f))
        for f in tmp_objects:
            if f.is_dir():
                shutil.rmtree(f)
            else:
                f.unlink()
        self._log_free_space('after', self._fs.tmp_dir)

    @_log_errors
    def _remove_old_tests_dirs(self):
        if not self._fs.tests_dir.exists():
            self._logger.debug('tests folder is not found')
            return

        self._log_free_space('before', self._fs.tests_dir)
        tests_dirs = sorted(
            [
                (f, f.stat().st_ctime)
                for f in self._fs.tests_dir.iterdir()
                if not self._is_forbiden_dir(f) and f.name != 'stpd-cache' and f.is_dir()
            ],
            key=lambda f: f[1],
        )
        for f, _ in tests_dirs:
            if self._get_free_space(self._fs.tests_dir) >= self._job_disk_limit:
                break
            shutil.rmtree(f)
        self._log_free_space('after', self._fs.tests_dir)

    @_log_errors
    def _remove_old_stpd_cache_files(self):
        if not self._stpd_cache_dir.exists():
            self._logger.debug('stpd cache folder is not found')
            return

        self._log_free_space('before', self._stpd_cache_dir)
        stpd_cache = sorted(
            [
                (f, f.stat().st_ctime)
                for f in self._stpd_cache_dir.iterdir()
                if not self._is_forbiden_dir(f) and f.is_file()
            ],
            key=lambda f: f[1],
        )

        time_threshold = time.time() - self._stpd_cache_ttl
        for f, f_ctime in stpd_cache:
            if f_ctime > time_threshold and self._get_free_space(self._fs.tests_dir) >= self._job_disk_limit:
                break
            f.unlink()
        self._log_free_space('after', self._stpd_cache_dir)

    @_log_errors
    def _clean_netort_resources(self):
        if self._resource_manager is None:
            self._logger.debug('resource manager is not set')
            return

        netort_dir = Path(self._resource_manager.tmp_path_prefix)
        if not netort_dir.exists():
            self._logger.debug('netort forlder is not exists')
            return

        self._log_free_space('before', netort_dir)
        netort_dirs = set(
            Path(op.factory.keywords['path_provider'].prefix)
            for op in self._resource_manager.openers
            if hasattr(op.factory, 'keywords') and 'path_provider' in op.factory.keywords
        )
        netort_objects = sorted(
            [(f, f.stat().st_ctime) for d in netort_dirs if d.exists() for f in d.rglob('*') if f.is_file()],
            key=lambda f: f[1],
        )

        time_threshold = time.time() - self._netort_cache_ttl
        for f, f_ctime in netort_objects:
            if f_ctime > time_threshold and self._get_free_space(netort_dir) >= self._job_disk_limit:
                break
            f.unlink()
        self._log_free_space('after', netort_dir)

    def cleanup(self):
        self._cleanup_temporary_dir()
        self._remove_old_stpd_cache_files()
        self._clean_netort_resources()
        self._remove_old_tests_dirs()
