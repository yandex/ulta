from typing import Protocol


class JobFinalizer(Protocol):
    def run(self):
        pass
