import logging
import os
import stat
import sys

from typing import Iterable
from ulta.common.config import UltaConfig


def create_default_logger(config: UltaConfig):
    if config.logging_path:
        handlers = [
            _create_file_handler(config.logging_path),
            _create_stdout_handler(logging.WARNING),
        ]
    else:
        handlers = [_create_stdout_handler()]
    logger = _init_logger(logging.getLogger('ulta'), handlers, logging.getLevelName(config.logging_level))
    logger.info('Log file created')
    return logger


def _init_logger(logger: logging.Logger, handlers: Iterable[logging.Handler], level) -> logging.Logger:
    logger.handlers = []
    logger.setLevel(level)
    for handler in handlers:
        handler.setFormatter(
            logging.Formatter('%(asctime)s [%(levelname)s] %(name)s %(filename)s:%(lineno)d\t%(message)s')
        )
        logger.addHandler(handler)
    return logger


def _create_file_handler(log_file_path: str):
    if os.path.dirname(log_file_path) == log_file_path:
        log_file_path = os.path.join(log_file_path, 'ulta.log')
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    open(log_file_path, 'a').close()
    current_file_mode = os.stat(log_file_path).st_mode
    os.chmod(log_file_path, current_file_mode | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
    return logging.FileHandler(log_file_path)


def _create_stdout_handler(level=None):
    handler = logging.StreamHandler(sys.stdout)
    if level is not None:
        handler.setLevel(level)
    return handler
