from typing import Protocol


class JobBackgroundWorker(Protocol):
    def start(self):
        pass

    def stop(self):
        pass

    def finish(self):
        pass


class JobFinalizer(Protocol):
    def run(self):
        pass
