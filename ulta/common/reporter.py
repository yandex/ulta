import logging
import typing
import uuid
from collections import deque, defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta
from queue import Empty
from threading import Lock
from ulta.common.background_worker import run_background_worker
from ulta.common.collections import QueueLike
from ulta.common.exceptions import CompositeException
from ulta.common.utils import now


class ReporterHandlerProtocol(typing.Protocol):
    def handle(self, request_id: str, data: typing.Any): ...

    def error_handler(self, error: Exception, logger: logging.Logger): ...


class Reporter:
    def __init__(
        self,
        *sources: QueueLike,
        logger: logging.Logger,
        handlers: ReporterHandlerProtocol | list[ReporterHandlerProtocol],
        retention_period: timedelta | None = None,
        max_batch_size: int = 1,
        report_interval: float = 5,
        max_unsent_size: int = 100_000,
        prepare_message: typing.Callable[[typing.Any], typing.Any] | None = None,
    ):
        self._sources = sources
        if not isinstance(handlers, list):
            handlers = [handlers]
        self._handlers = handlers
        self._retention_period = retention_period or timedelta(hours=1)
        self._max_batch_size = max_batch_size
        self._report_interval = report_interval
        # deque appends and pops are atomic and declared thread safe
        # https://docs.python.org/3/library/collections.html#deque-objects
        self._unsent_messages: dict[int, deque[_UnsentMessage]] = defaultdict(lambda: deque(maxlen=max_unsent_size))
        self._lock = Lock()
        self._logger = logger
        self._prepare_message = prepare_message

    def add_sources(self, *sources: QueueLike):
        self._sources = self._sources + sources

    @contextmanager
    def run(self):
        try:
            with run_background_worker(self.report, self._error_handler, self._report_interval) as stop:
                yield stop
        finally:
            try:
                self.report()
            except BaseException:
                self._logger.exception('Failed to report STOPPED status')

    def report(self):
        with self._lock:
            records = []
            for source in self._sources:
                try:
                    while not source.empty():
                        item = source.get_nowait()

                        try:
                            if self._prepare_message is not None:
                                item = self._prepare_message(item)
                        except Exception as e:
                            self._logger.warning('Failed to prepare log message: %s', e)
                        else:
                            records.append(item)
                except Empty:
                    pass

        to_send = [_UnsentMessage(d) for d in _chop(records, self._max_batch_size)]
        errors = []
        for handler in self._handlers:
            handler_to_send = self._get_and_release_unsent(handler) + to_send

            for item in handler_to_send:
                if not item:
                    continue
                try:
                    handler.handle(item.id, item.data)
                except Exception as e:
                    self._put_unsent(handler, item)
                    errors.append(e)
            if len(errors) > 1:
                handler.error_handler(CompositeException(errors), self._logger)
            elif len(errors) == 1:
                handler.error_handler(errors[0], self._logger)

    def _error_handler(self, error: Exception):
        return self._logger.error('Unhandled error occured in Reporter thread', exc_info=error)

    def _put_unsent(self, handler: object, unsent_message: typing.Any):
        unsent_queue = self._unsent_messages[id(handler)]
        if isinstance(unsent_message, _UnsentMessage):
            unsent_queue.append(unsent_message)
        else:
            unsent_queue.append(_UnsentMessage(unsent_message))

    def _get_and_release_unsent(self, handler: object) -> list["_UnsentMessage"]:
        result = []
        retention_timestamp = (datetime.now() - self._retention_period).timestamp()
        unsent_queue = self._unsent_messages[id(handler)]
        while len(self._unsent_messages) > 0:
            try:
                msg = unsent_queue.popleft()
                if msg.ts >= retention_timestamp:
                    result.append(msg)
            except IndexError:
                break
        return result


def _chop(data: list, size: int) -> list[list]:
    if size <= 0:
        size = len(data) + 1
    n, rem = divmod(len(data), size)
    chunks = [data[i * size : (i + 1) * size] for i in range(n)]
    if rem:
        chunks.append(data[-rem:])
    return chunks


class _UnsentMessage:
    def __init__(self, data) -> None:
        self.id = str(uuid.uuid4())
        self.ts = now().timestamp()
        self.data = data


class NullReporter:
    def report(self):
        return

    @contextmanager
    def run(self):
        yield
