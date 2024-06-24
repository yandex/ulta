import logging
import logging.handlers
import os
import stat
import sys

from queue import Queue, Full
from ulta.common.config import UltaConfig


class LogMessage:
    def __init__(self, record: logging.LogRecord, labels: dict[str, str | None]):
        self.record = record
        self.message = record.getMessage()
        self.level = record.levelno
        self.timestamp = int(record.created)
        self.labels = labels


def init_logging(config: UltaConfig) -> logging.Logger:
    logger = logging.getLogger()
    logger.handlers = []
    logger.setLevel(config.log_level or logging.INFO)
    for handler in _create_default_handlers(config):
        handler.setFormatter(
            logging.Formatter('%(asctime)s [%(levelname)s] %(name)s %(filename)s:%(lineno)d\t%(message)s')
        )
        logger.addHandler(handler)
    return logger


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

    stdout_log_level = logging.WARNING if handlers else None
    handlers.append(_create_stdout_handler(stdout_log_level))
    handlers.append(_create_sink_handler(max_queue_size=100_000))
    return handlers


def _create_file_handler(log_file_path: str):
    if os.path.isdir(log_file_path) or os.path.dirname(log_file_path) + '/' == log_file_path:
        log_file_path = os.path.join(log_file_path, 'ulta.log')
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    open(log_file_path, 'a').close()
    current_file_mode = os.stat(log_file_path).st_mode
    os.chmod(log_file_path, current_file_mode | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
    return logging.FileHandler(log_file_path)


def _create_stdout_handler(level: int | None = None):
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level if level is not None else logging.INFO)
    return handler


class SinkHandler(logging.Handler):
    def __init__(self, sink: Queue):
        logging.Handler.__init__(self)
        self._sink = sink

    def emit(self, record: logging.LogRecord):
        try:
            self._sink.put_nowait(record)
        except Full:
            pass

    def flush(self):
        pass


def _create_sink_handler(level: int | None = None, max_queue_size: int = 0):
    # TODO: use priority deque to drop debug messages first on overflow
    handler = SinkHandler(Queue(max_queue_size))
    if level is not None:
        handler.setLevel(level)
    return handler
