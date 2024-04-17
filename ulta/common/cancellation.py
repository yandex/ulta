from enum import IntEnum
from threading import Lock


class CancellationRequest(Exception):
    pass


class CancellationType(IntEnum):
    NOT_SET = 0
    GRACEFUL = 1
    FORCED = 2


class Cancellation:
    def __init__(self):
        self._reason = ''
        self._current_type = CancellationType.NOT_SET
        self._lock = Lock()

    def notify(self, reason: str, level: CancellationType = CancellationType.GRACEFUL):
        with self._lock:
            if level > self._current_type:
                self._current_type = level
            self._reason = reason

    def is_set(self, cancellation_type: CancellationType = CancellationType.GRACEFUL) -> bool:
        return CancellationType(self._current_type) >= cancellation_type

    def raise_on_set(self, cancellation_type: CancellationType = CancellationType.GRACEFUL) -> bool:
        if self.is_set(cancellation_type=cancellation_type):
            raise CancellationRequest(self.explain())
        return False

    def explain(self) -> str:
        return self._reason
