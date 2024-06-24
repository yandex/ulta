import typing
from contextlib import contextmanager
from threading import Event, Thread


@contextmanager
def run_background_worker(
    iteration: typing.Callable[[], None], error_handler: typing.Callable[[Exception], None], interval: float
):
    try:
        thread, stop = _run_thread(iteration, error_handler, interval)
        yield stop
    finally:
        if stop is not None:
            stop.set()
        if thread is not None:
            thread.join()


def _run_thread(
    iteration: typing.Callable[[], None], error_handler: typing.Callable[[Exception], None], interval: float
):
    stop = Event()

    def worker():
        while not stop.is_set():
            try:
                iteration()
            except Exception as e:
                error_handler(e)
            finally:
                stop.wait(interval)

    thread = Thread(target=worker)
    thread.start()
    return thread, stop
