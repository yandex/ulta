import logging
import logging.handlers
import os
import stat
import sys

from queue import Queue, Full
from ulta.common.config import UltaConfig


class LogMessage:
    def __init__(self, *, message: str, labels: dict[str, str], level: int, created_at: float):
        self.message = message
        self.level = level
        self.labels = labels
        self.created_at = created_at


def init_logging(config: UltaConfig) -> logging.Logger:
    logger = get_root_logger()
    logger.handlers = []
    for handler in _create_default_handlers(config):
        logger.addHandler(handler)

    try:
        logger.setLevel(config.log_level or logging.INFO)
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


def get_event_logger() -> logging.Logger:
    logger = get_logger('events')
    logger.propagate = True
    return logger


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


def _create_file_handler(log_file_path: str):
    if os.path.isdir(log_file_path) or os.path.dirname(log_file_path) + '/' == log_file_path:
        log_file_path = os.path.join(log_file_path, 'ulta.log')
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    open(log_file_path, 'a').close()
    current_file_mode = os.stat(log_file_path).st_mode
    os.chmod(log_file_path, current_file_mode | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
    handler = logging.FileHandler(log_file_path)
    handler.setFormatter(_create_default_formatter())
    return handler


def _create_stdout_handler():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_create_default_formatter())
    return handler


def _create_default_formatter():
    return logging.Formatter('%(asctime)s [%(levelname)s] %(name)s %(filename)s:%(lineno)d\t%(message)s')


class SinkHandler(logging.Handler):
    def __init__(self, sink: Queue):
        logging.Handler.__init__(self)
        self.sink = sink

    def emit(self, record: logging.LogRecord):
        try:
            self.sink.put_nowait(record)
        except Full:
            pass

    def flush(self):
        pass


def create_sink_handler(level: int | None = None, max_queue_size: int = 0):
    # TODO: use priority deque to drop debug messages first on overflow
    handler = SinkHandler(Queue(max_queue_size))
    if level is not None:
        handler.setLevel(level)
    return handler
