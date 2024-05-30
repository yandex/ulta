import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from ulta.common.cancellation import Cancellation
from ulta.common.config import UltaConfig
from ulta.common.healthcheck import HealthCheckProtocol
from ulta.common.state import State, GenericObserver
from yandextank.contrib.netort.netort import process


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


class NotEnoughFreeSpace(Exception):
    ...


class FileSystemObserver(HealthCheckProtocol):
    @dataclass
    class _FSUsage:
        size: int
        used: int
        available: int
        mount: str

    def __init__(
        self,
        fs: FS,
        state: State,
        logger: logging.Logger,
        cancellation: Cancellation,
    ):
        self._logger = logger
        self._observer = GenericObserver(state, logger, cancellation)
        self._fs = fs
        self._use_native = True
        self._use_fallback = True
        self._requirements = {
            self._fs.tmp_dir.as_posix(): parse_bytes('2G'),
            self._fs.tests_dir.as_posix(): parse_bytes('2G'),
            self._fs.lock_dir.as_posix(): parse_bytes('1M'),
        }

    def healthcheck(self):
        for d, req in self._requirements.items():
            with self._observer.observe(stage=f'check working dir {str(d)}'):
                ensure_dir(d)

        fs_usage = self._get_fs_usage(list(self._requirements.keys()))
        for d, req in self._requirements.items():
            with self._observer.observe(stage=f'check free space {str(d)}', exceptions=NotEnoughFreeSpace):
                self._check_free_space(d, req, fs_usage.get(d))

    def _check_free_space(self, dir_name: str, requirement: int, usage: _FSUsage | None):
        if requirement == -1:
            return

        if not self._use_native and not self._use_fallback:
            self._logger.debug('filesystem healthcheck skip: native and fallback approach unavailable')
            return

        if usage is None:
            self._logger.warning('Unable to find free space info for dir %s', dir_name)
            return

        self._logger.debug('usage_found %s', usage)
        if usage.available != -1 and usage.available < requirement:
            raise NotEnoughFreeSpace(
                f'Agent has not enough free space for dir "{dir_name}": {format_bytes(usage.available)}; required minimum {format_bytes(requirement)}'
            )

    def _get_fs_usage(self, paths: list[str]) -> dict[str, _FSUsage]:
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

    def _get_fs_usage_native(self, paths: list[str]) -> dict[str, _FSUsage]:
        result = {}
        for path in paths:
            size, used, avail = shutil.disk_usage(path)
            result[path] = FileSystemObserver._FSUsage(size, used, avail, path)
        return result

    def _get_fs_usage_with_df(self, paths: list[str]) -> dict[str, _FSUsage]:
        fs_usage_result = self._run_fs_usage_tool()

        all_mounts: dict[str, FileSystemObserver._FSUsage] = {}
        fs_info = fs_usage_result.split('\n')
        for fs_item in fs_info[1:]:
            if fs_item:
                fs_item_info = [i for i in fs_item.split() if i.strip()]
                assert len(fs_item_info) == 6, ' '.join(fs_item_info)
                all_mounts[fs_item_info[0]] = FileSystemObserver._FSUsage(
                    size=parse_bytes(fs_item_info[1]),
                    used=parse_bytes(fs_item_info[2]),
                    available=parse_bytes(fs_item_info[3]),
                    mount=fs_item_info[5],
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
