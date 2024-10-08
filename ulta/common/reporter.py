import logging
import typing
import uuid
from collections import deque
from contextlib import contextmanager
from datetime import datetime, timedelta
from queue import Queue, Empty
from threading import Lock
from ulta.common.background_worker import run_background_worker
from ulta.common.exceptions import CompositeException
from ulta.common.utils import now


class Reporter:
    def __init__(
        self,
        *sources: Queue,
        logger: logging.Logger,
        handler: typing.Callable[[str, typing.Any], None],
        error_handler: typing.Callable[[Exception], None],
        retention_period: timedelta | None = None,
        max_batch_size: int = 1,
        report_interval: float = 5,
        max_unsent_size: int = 100_000,
        prepare_message: typing.Callable[[typing.Any], typing.Any] | None = None,
    ):
        self._sources = sources
        self._handler = handler
        self._error_handler = error_handler
        self._retention_period = retention_period or timedelta(hours=1)
        self._max_batch_size = max_batch_size
        self._report_interval = report_interval
        # deque appends and pops are atomic and declared thread safe
        # https://docs.python.org/3/library/collections.html#deque-objects
        self._unsent_messages: deque[_UnsentMessage] = deque(maxlen=max_unsent_size)
        self._lock = Lock()
        self._logger = logger
        self._prepare_message = prepare_message

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

            to_send = self._get_and_release_unsent() + [_UnsentMessage(d) for d in _chop(records, self._max_batch_size)]

        errors = []
        for item in to_send:
            if not item:
                continue
            try:
                self._handler(item.id, item.data)
            except Exception as e:
                self._put_unsent(item)
                errors.append(e)
        if len(errors) > 1:
            raise CompositeException(errors)
        elif len(errors) == 1:
            raise errors[0]

    def _put_unsent(self, unsent_message: typing.Any):
        if isinstance(unsent_message, _UnsentMessage):
            self._unsent_messages.append(unsent_message)
        else:
            self._unsent_messages.append(_UnsentMessage(unsent_message))

    def _get_and_release_unsent(self) -> list["_UnsentMessage"]:
        result = []
        retention_timestamp = (datetime.now() - self._retention_period).timestamp()
        while len(self._unsent_messages) > 0:
            try:
                msg = self._unsent_messages.popleft()
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
