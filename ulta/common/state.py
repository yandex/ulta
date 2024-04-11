from contextlib import contextmanager
from datetime import datetime
from dataclasses import dataclass
from ulta.common.cancellation import Cancellation
from ulta.common.utils import now
import logging


@dataclass
class StateError:
    updated_at: datetime
    stage: str
    message: str

    def _hash(self):
        return self.stage + self.message


class State:
    def __init__(self):
        self._active_errors: dict[str, StateError] = {}

    @property
    def ok(self) -> bool:
        return not bool(self.current_errors())

    def get_summary_message(self) -> str:
        return '\n'.join(format_error(e) for e in self.current_errors())

    def current_errors(self) -> list[StateError]:
        return list(self._active_errors.values())

    def error(self, stage: str, error: str | Exception):
        new_error = StateError(updated_at=now(), stage=stage, message=str(error))
        self._active_errors[new_error._hash()] = new_error

    def cleanup(self, stage: str):
        self._active_errors = {e._hash(): e for e in self._active_errors.values() if e.stage != stage}


def format_error(error: StateError) -> str:
    return error.message


class Observer(State):
    def __init__(
        self,
        logger: logging.Logger,
        cancellation: Cancellation,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self._logger = logger
        self._cancellation = cancellation

    @contextmanager
    def observe(self, *, stage: str, critical: bool = True, exception: type | tuple[type, ...] | None = None):
        exception = exception or Exception
        try:
            yield
        except exception as e:
            msg = f'The error occured at "{stage}": {str(e)}'
            if critical:
                self._logger.error('The critical error occured: %s. Notifying service termination...', msg)
                self._cancellation.notify(msg)
            else:
                self._logger.info('Noncritical error: %s.', msg)
            self.error(stage, msg)
        else:
            self.cleanup(stage)

    def observe_file_system(self, stage: str = ''):
        stage = stage or 'access file system'
        return self.observe(stage=stage, critical=True, exception=(PermissionError, OSError))
