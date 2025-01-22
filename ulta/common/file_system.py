from collections.abc import Callable
import contextlib
import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
import time
from ulta.common.cancellation import Cancellation
from ulta.common.config import UltaConfig
from ulta.common.healthcheck import HealthCheckProtocol
from ulta.common.state import State, GenericObserver
from ulta.common.job import Job, JobPluginType
from yandextank.contrib.netort.netort import process
from yandextank.contrib.netort.netort.resource import ResourceManager


@dataclass
class FS:
    tmp_dir: Path
    tests_dir: Path
    lock_dir: Path


def make_fs_from_ulta_config(config: UltaConfig) -> FS:
    return FS(
        tmp_dir=Path(os.path.join(config.work_dir, '_tmp')),
        tests_dir=Path(os.path.join(config.work_dir, 'tests')),
        lock_dir=Path(config.lock_dir),
    )


def ensure_dir(dir_path: Path | str, permissions: int = os.W_OK | os.R_OK | os.X_OK) -> Path:
    path = Path(dir_path)
    path.mkdir(parents=True, exist_ok=True)
    if permissions > 0:
        if not os.access(dir_path, permissions):
            raise PermissionError(f'Permission denied for path {dir_path}')
    return path


class FilesystemUsage:
    @dataclass
    class FSUsage:
        size: int
        used: int
        available: int
        mount: Path

    def __init__(self, logger: logging.Logger):
        self._logger = logger
        self._use_native = True
        self._use_fallback = True

    def get(self, path: Path) -> FSUsage:
        return self.get_batch([path])[path]

    def get_batch(self, paths: list[Path]) -> dict[Path, FSUsage]:
        if self._use_native:
            try:
                return self._get_fs_usage_native(paths)
            except Exception:
                self._logger.exception('_get_fs_usage native approach failed')
                self._use_native = False

        if self._use_fallback:
            try:
                return self._get_fs_usage_with_df(paths)
            except Exception:
                self._logger.exception('_get_fs_usage fallback approach failed')
                self._use_fallback = False

        return {}

    def _get_fs_usage_native(self, paths: list[Path]) -> dict[Path, FSUsage]:
        result = {}
        for path in paths:
            size, used, avail = shutil.disk_usage(path)
            result[path] = FilesystemUsage.FSUsage(size, used, avail, path)
        return result

    def _get_fs_usage_with_df(self, paths: list[Path]) -> dict[Path, FSUsage]:
        fs_usage_result = self._run_fs_usage_tool()

        all_mounts: dict[str, FilesystemUsage.FSUsage] = {}
        fs_info = fs_usage_result.split('\n')
        for fs_item in fs_info[1:]:
            if fs_item:
                fs_item_info = [i for i in fs_item.split() if i.strip()]
                assert len(fs_item_info) == 6, ' '.join(fs_item_info)
                all_mounts[fs_item_info[0]] = FilesystemUsage.FSUsage(
                    size=parse_bytes(fs_item_info[1]),
                    used=parse_bytes(fs_item_info[2]),
                    available=parse_bytes(fs_item_info[3]),
                    mount=Path(fs_item_info[5]),
                )

        result = {}
        for path in paths:
            usage_found = None
            usage_found_mount_path = None
            for _, usage in all_mounts.items():
                mount_path = Path(usage.mount)
                if Path(path).is_relative_to(mount_path):
                    if usage_found_mount_path is None or mount_path.is_relative_to(usage_found_mount_path):
                        usage_found = usage
                        usage_found_mount_path = mount_path
            result[path] = usage_found
        return result

    def _run_fs_usage_tool(self) -> str:
        exit_code, stdout, stderr = process.execute(
            'df -l -B1 -x fuse -x tmpfs -x devtmpfs', shell=False, catch_out=True
        )
        if exit_code != 0:
            # process.execute does all the logging
            raise Exception('df failed')
        return stdout.decode('utf8')


DEFAULT_STPD_CACHE_TTL = 7 * 86400
DEFAULT_NETORT_CACHE_TTL = 7 * 86400

ROOT_DIR = Path('/')


class FilesystemCleanup:
    def __init__(
        self,
        logger: logging.Logger,
        fs: FS,
        job: Job | None,
        resource_manager: ResourceManager | None,
        stpd_cache_ttl=DEFAULT_STPD_CACHE_TTL,
        netort_cache_ttl=DEFAULT_NETORT_CACHE_TTL,
    ):
        self._logger = logger.getChild('FilesystemCleanup')
        self._fs = fs
        self._job = job

        self._fs_usage = FilesystemUsage(self._logger)

        self._job_disk_limit = self._get_job_disk_limit()
        self._stpd_cache_dir = self._fs.tests_dir / 'stpd-cache'
        self._stpd_cache_ttl = stpd_cache_ttl

        self._netort_dir = Path(resource_manager.tmp_path_prefix) if resource_manager is not None else None
        self._netort_cache_ttl = netort_cache_ttl

        self._forbiden_dirs: set[Path] = {
            self._stpd_cache_dir,
            self._fs.tests_dir,
            self._fs.tmp_dir,
            self._fs.tests_dir / 'lunapark',
        }
        if self._job is not None:
            if self._job.test_data_dir:
                self._forbiden_dirs.add(Path(self._job.test_data_dir))
                self._forbiden_dirs.add(Path(self._job.test_data_dir.replace('/test_data_', '/')))
            if self._job.artifact_dir_path:
                self._forbiden_dirs.add(Path(self._job.artifact_dir_path))
        self._forbiden_dirs = {d.resolve() for d in self._forbiden_dirs if d.exists()}

    def _get_job_disk_limit(self) -> int:
        job_disk_limit = 2048
        if self._job is not None:
            if rc := self._job.get_plugins(JobPluginType.RESOURCE_CHECK):
                job_disk_limit = int(rc[0][1].get('disk_limit', 2048))
        if job_disk_limit <= 0:
            job_disk_limit = 2048
        # extra 100M bytes for possible miscalculations of ResourceCheck plugin
        # due to different approach to free space evaluation
        job_disk_limit += 100
        job_disk_limit *= 1024 * 1024
        return job_disk_limit

    def _log_free_space(self, stage: str, path: Path) -> int:
        free_space = self._fs_usage.get(path).available
        self._logger.debug(
            'free space %(stage)s cleanup %(path)s: %(free_space)s',
            dict(stage=stage, path=path, free_space=free_space),
        )
        return free_space

    def _is_good_dir(self, path: Path, title: str) -> bool:
        path = path.resolve()
        if not path.exists() or not path.is_dir():
            self._logger.warning(f'{title} folder is not found')
            return False
        if path == ROOT_DIR:
            self._logger.warning(f'{title} folder is root')
            return False
        return True

    def _is_forbiden(self, path: Path) -> bool:
        return path.resolve() in self._forbiden_dirs or path.name == 'stpd-cache'

    def _do_clean(
        self, title: str, path: Path, collect_objects: Callable[[Path], list[Path]], ttl: int = 0, disk_limit: int = 0
    ) -> int:
        try:
            if not self._is_good_dir(path, title):
                return -1

            fs_objects = sorted(
                [(f, f.stat().st_ctime) for f in collect_objects(path)],
                key=lambda f: f[1],
            )

            self._log_free_space('before', path)

            time_threshold = time.time() - ttl
            for f, f_ctime in fs_objects:
                if self._fs_usage.get(path).available > disk_limit and (ttl <= 0 or time_threshold < f_ctime):
                    break
                if f.is_dir():
                    shutil.rmtree(f)
                else:
                    f.unlink()

            free_space = self._log_free_space('after', path)
            return free_space
        except Exception as e:
            self._logger.error('error during cleanup %(title)s: {ex}', {'title': title, 'ex': str(e)})
        return -1

    def clean_temporary_dir(self, disk_limit: int = -1) -> int:
        return self._do_clean(
            'temporary',
            self._fs.tmp_dir,
            lambda path: [p for p in path.iterdir() if not self._is_forbiden(p)],
            disk_limit=disk_limit,
        )

    def clean_tests_dirs(self, disk_limit: int = -1) -> int:
        return self._do_clean(
            'tests folder',
            self._fs.tests_dir,
            lambda path: [p for p in path.iterdir() if p.is_dir() and not self._is_forbiden(p)],
            disk_limit=disk_limit,
        )

    def clean_stpd_cache_files(self, disk_limit: int = -1) -> int:
        return self._do_clean(
            'stpd cache',
            self._stpd_cache_dir,
            lambda path: [p for p in path.iterdir() if p.is_file() and not self._is_forbiden(p)],
            self._stpd_cache_ttl,
            disk_limit=disk_limit,
        )

    def clean_netort_resources(self, disk_limit: int = -1) -> int:
        if self._netort_dir is None:
            return -1

        return self._do_clean(
            'netort cache',
            self._netort_dir,
            lambda path: [p for p in path.rglob('*downloaded_resource*') if p.is_file() and not self._is_forbiden(p)],
            self._netort_cache_ttl,
            disk_limit=disk_limit,
        )

    def cleanup(self, disk_limit: int = -1):
        if disk_limit < 0:
            disk_limit = self._job_disk_limit

        self.clean_temporary_dir(disk_limit)
        self.clean_stpd_cache_files(disk_limit)
        self.clean_netort_resources(disk_limit)
        self.clean_tests_dirs(disk_limit)


class NotEnoughFreeSpace(Exception): ...


class FileSystemObserver(HealthCheckProtocol):
    @dataclass
    class Requirement:
        path: Path
        size: int
        cleanup: Callable[[int], int]

    def __init__(
        self,
        fs: FS,
        state: State,
        mutable_activity_lock: Callable[..., contextlib.AbstractContextManager[bool]],
        logger: logging.Logger,
        cancellation: Cancellation,
    ):
        self._logger = logger
        self._observer = GenericObserver(state, logger, cancellation)
        self._mutable_activity_lock = mutable_activity_lock
        self._fs = fs
        self._fs_utils = FilesystemUsage(logger)
        self._fs_cleanup = FilesystemCleanup(logger, fs, None, None)
        self._requirements = [
            FileSystemObserver.Requirement(self._fs.tmp_dir, parse_bytes('2G'), self._fs_cleanup.clean_temporary_dir),
            FileSystemObserver.Requirement(self._fs.tests_dir, parse_bytes('2G'), self._fs_cleanup.clean_tests_dirs),
            FileSystemObserver.Requirement(self._fs.lock_dir, parse_bytes('1M'), lambda disk_limit: -1),
        ]

    def healthcheck(self):
        for req in self._requirements:
            with self._observer.observe(stage=f'check working dir {req.path}', critical=True):
                ensure_dir(req.path)

        fs_usage = self._fs_utils.get_batch([req.path for req in self._requirements])
        for req in self._requirements:
            with self._observer.observe(
                stage=f'check free space {req.path}', critical=False, exceptions=NotEnoughFreeSpace
            ):
                self._check_free_space(req, fs_usage.get(req.path))

    def _check_free_space(self, requirement: Requirement, usage: FilesystemUsage.FSUsage | None):
        if requirement.size == -1:
            return

        if usage is None or usage.available < 0:
            self._logger.warning('Unable to find free space info for dir %s', requirement.path)
            return

        self._logger.debug('usage_found %s', usage)
        if usage.available < requirement.size:
            with self._mutable_activity_lock(blocking=False) as mutating_avalable:
                if mutating_avalable:
                    self._logger.debug(
                        f'Trying to free space in dir "{requirement.path}": available {format_bytes(usage.available)}'
                        f' is less than required {format_bytes(requirement.size)}'
                    )
                    available = requirement.cleanup(requirement.size)
                else:
                    available = -1
            if available < 0 or available < requirement.size:
                raise NotEnoughFreeSpace(
                    f'Agent has not enough free space for dir "{requirement.path}": {format_bytes(usage.available)}; '
                    f'required minimum {format_bytes(requirement.size)}'
                )


BYTE_SUFFIXES = {'k': 2**10, 'K': 2**10, 'M': 2**20, 'G': 2**30, 'T': 2**40, 'P': 2**50}


def format_bytes(value: float) -> str:
    suffixes = ['', 'K', 'M', 'G', 'T', 'P']
    suffix = 0
    while value >= 2**10 and suffix < len(suffixes) - 1:
        value = value / (2**10)
        suffix += 1
    return str(int(value)) + suffixes[suffix]


def parse_bytes(s: str) -> int:
    if len(s) == 0:
        return -1

    try:
        suffix = s[-1]
        if suffix in BYTE_SUFFIXES:
            return int(float(s[:-1]) * BYTE_SUFFIXES[suffix])
        else:
            return int(s)
    except ValueError:
        logging.warning('Failed to parse byte value %s', s)
        return -1
