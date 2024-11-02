import multiprocessing
import typing
from collections import deque
from queue import Empty, Full, Queue


class Deque[T]:
    def __init__(self, maxlen: int | None = None):
        self._q = deque(maxlen=maxlen)

    def empty(self) -> bool:
        return len(self._q) == 0

    def get_nowait(self) -> T:
        if self.empty():
            raise Empty()
        return self._q.popleft()

    def put_nowait(self, item: T) -> None:
        if self._q.maxlen is not None and self._q.maxlen > 0 and len(self._q) >= self._q.maxlen:
            raise Full()
        self._q.append(item)


QueueLike = typing.Union[Deque, Queue, multiprocessing.Queue]
