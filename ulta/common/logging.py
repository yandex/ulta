import logging
import logging.handlers
import os
import stat
import sys
import typing

from queue import Full
from ulta.common.config import UltaConfig
from ulta.common.collections import QueueLike
from ulta.common.utils import TrackRequestHeaders, exception_grpc_metadata, str_to_loglevel


def init_logging(config: UltaConfig) -> logging.Logger:
    logger = get_root_logger()
    logger.handlers = []
    for handler in _create_default_handlers(config):
        logger.addHandler(handler)

    try:
        logger.setLevel(str_to_loglevel(config.log_level, logging.INFO))
    except ValueError:
        logger.setLevel(logging.INFO)
        logger.error('Invalid log level value: %s', config.log_level)

    return get_logger()


def get_root_logger() -> logging.Logger:
    return logging.getLogger()


def get_logger(name: str = 'ulta') -> logging.Logger:
    if name != 'ulta':
        name = 'ulta.' + name
    return logging.getLogger(name)


def _create_default_handlers(config: UltaConfig) -> list[logging.Handler]:
    handlers = []
    if config.log_path:
        try:
            handlers.append(_create_file_handler(config.log_path))
        except Exception as e:
            try:
                sys.stderr.write(f'Unable to create file logger: {str(e)}\n')
                sys.stderr.flush()
            except Exception:
                pass

    stdout_handler_factory = config.custom_stdout_log_handler_factory or _create_stdout_handler
    handler = stdout_handler_factory()
    handlers.append(handler)
    return handlers


def _create_file_handler(log_file_path: str) -> logging.Handler:
    if os.path.isdir(log_file_path) or os.path.dirname(log_file_path) + '/' == log_file_path:
        log_file_path = os.path.join(log_file_path, 'ulta.log')
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    open(log_file_path, 'a').close()
    current_file_mode = os.stat(log_file_path).st_mode
    os.chmod(log_file_path, current_file_mode | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
    handler = logging.FileHandler(log_file_path)
    handler.setFormatter(_create_default_formatter())
    return handler


def _create_stdout_handler() -> logging.Handler:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_create_default_formatter())
    return handler


def _exception_trackers_str(err: Exception) -> str | None:
    grpc_metadata = exception_grpc_metadata(err)
    if grpc_metadata is not None:
        headers = TrackRequestHeaders.from_grpc_metadata(grpc_metadata)
        if not any(bool(v) for _, v in headers.items()):
            return None

        fence_str = '###############'
        data_str = '\n'.join(f'# {k}: {v or ""}' for k, v in headers.items())
        return f'{fence_str}\n{data_str}\n{fence_str}'

    return None


class _DefaultFormatter(logging.Formatter):
    def formatException(self, ei) -> str:
        res = super().formatException(ei)
        if isinstance(ei[1], Exception):
            if trackers_str := _exception_trackers_str(ei[1]):
                res += '\n'
                res += trackers_str

        return res


def _create_default_formatter():
    return _DefaultFormatter('%(asctime)s [%(levelname)s] %(name)s %(filename)s:%(lineno)d\t%(message)s')


class SinkHandler(logging.Handler):
    def __init__(
        self, sink: QueueLike, transformer: typing.Callable[[logging.LogRecord], logging.LogRecord] | None = None
    ):
        logging.Handler.__init__(self)
        self.sink = sink
        self.transformer = transformer

    def emit(self, record: logging.LogRecord):
        try:
            if self.transformer is not None:
                record = self.transformer(record)
            self.sink.put_nowait(record)
        except Full:
            pass

    def flush(self):
        pass
