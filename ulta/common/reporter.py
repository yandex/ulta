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

    def get_max_batch_size(self) -> int | None: ...


class Reporter:
    def __init__(
        self,
        *sources: QueueLike,
        logger: logging.Logger,
        handlers: ReporterHandlerProtocol | list[ReporterHandlerProtocol],
        retention_period: timedelta | None = None,
        report_interval: float = 5,
        max_unsent_size: int = 1000,
        use_exponential_backoff: bool = False,
    ):
        self._sources = sources
        if not isinstance(handlers, list):
            handlers = [handlers]
        self._handlers = handlers
        self._retention_period = retention_period or timedelta(hours=1)
        self._report_interval = report_interval
        # deque appends and pops are atomic and declared thread safe
        # https://docs.python.org/3/library/collections.html#deque-objects
        self._unsent_messages: dict[int, deque[_UnsentMessage]] = defaultdict(lambda: deque(maxlen=max_unsent_size))
        self._lock = Lock()
        self._logger = logger
        self._max_unsent_size = max_unsent_size
        if use_exponential_backoff:
            self._handler_managers: dict[int, _AttemptManager] = defaultdict(_AttemptManager)
        else:
            self._handler_managers: dict[int, _AttemptManager] = defaultdict(_DummyAttemptManager)

    def add_sources(self, *sources: QueueLike):
        self._sources = self._sources + sources

    @contextmanager
    def run(self):
        try:
            with run_background_worker(self.report, self._error_handler, self._report_interval) as stop:
                yield stop
        finally:
            try:
                self.report(force=True)
            except BaseException:
                self._logger.exception('Failed to report STOPPED status')

    def report(self, force: bool = False):
        records = self._collect_new_messages()
        for handler in self._handlers:
            attempt_manager = self._handler_managers[id(handler)]
            if not force and not attempt_manager.can_attempt():
                self._put_unsent(handler, records)
                continue

            errors = []
            all_handler_messages = sorted(self._get_and_release_unsent(handler) + records, key=lambda r: r.ts)
            to_send: list[list[_UnsentMessage]] = _chop(all_handler_messages, handler.get_max_batch_size() or 1)

            for unsent_chunk in to_send:
                if not unsent_chunk:
                    continue
                try:
                    handler.handle(str(uuid.uuid4()), [d.data for d in unsent_chunk])
                except Exception as e:
                    self._put_unsent(handler, unsent_chunk)
                    errors.append(e)

            attempt_manager.record(len(errors) == len(to_send))
            if len(errors) > 1:
                handler.error_handler(CompositeException(errors), self._logger)
            elif len(errors) == 1:
                handler.error_handler(errors[0], self._logger)

    def _error_handler(self, error: Exception):
        return self._logger.error('Unhandled error occurred in Reporter thread', exc_info=error)

    def _collect_new_messages(self) -> list['_UnsentMessage']:
        records = []
        with self._lock:
            for source in self._sources:
                try:
                    while not source.empty():
                        records.append(_UnsentMessage(source.get_nowait()))
                except Empty:
                    pass
        return records

    def _put_unsent(self, handler: object, unsent_chunk: list['_UnsentMessage']):
        self._unsent_messages[id(handler)].extend(unsent_chunk)

    def _get_and_release_unsent(self, handler: object) -> list['_UnsentMessage']:
        retention_timestamp = (datetime.now() - self._retention_period).timestamp()
        items = [m for m in self._unsent_messages[id(handler)] if m.ts >= retention_timestamp]
        self._unsent_messages[id(handler)].clear()
        return items


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
        self.ts = now().timestamp()
        self.data = data


class NullReporter:
    def report(self):
        return

    @contextmanager
    def run(self):
        yield


class _DummyAttemptManager:
    def can_attempt(self):
        return True

    def record(self, failure: bool):
        pass


class _AttemptManager:
    def __init__(
        self,
        multiplier=2,
        max_delay=600,
        base_delay=2,
    ):
        self._next_attempt: float = 0
        self._multiplier = multiplier
        self._max_delay = max_delay
        self._base_delay = base_delay
        self._current_delay = base_delay

    def can_attempt(self):
        return self._next_attempt <= datetime.now().timestamp()

    def record(self, failure: bool):
        if not failure:
            self._next_attempt = 0
            self._current_delay = self._base_delay
            return

        delay = min(self._current_delay, self._max_delay)
        self._next_attempt = datetime.now().timestamp() + delay
        self._current_delay = min(self._current_delay * self._multiplier, self._max_delay)
