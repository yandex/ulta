from contextlib import contextmanager
from datetime import datetime
from dataclasses import dataclass
from ulta.common.cancellation import Cancellation
from ulta.common.logging import get_logger
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
    def __init__(self, logger: logging.Logger | None = None):
        self._active_errors: dict[str, StateError] = {}
        self._logger = logger or get_logger()
        self._state_stack = []

    @property
    def ok(self) -> bool:
        return not bool(self.current_errors())

    def get_summary_message(self) -> str:
        return '\n'.join(format_error(e) for e in self.current_errors())

    def current_errors(self) -> list[StateError]:
        return list(self._active_errors.values())

    def current_state(self) -> list[str]:
        return [s for s in self._state_stack]

    def error(self, stage: str, error: str | Exception):
        new_error = StateError(updated_at=now(), stage=stage, message=str(error))
        self._active_errors[new_error._hash()] = new_error
        self._logger.error(new_error.message, {'stage': new_error.stage})

    def cleanup(self, stage: str):
        self._active_errors = {e._hash(): e for e in self._active_errors.values() if e.stage != stage}

    @contextmanager
    def enter_state(self, name: str):
        try:
            self._state_stack.append(name)
            yield
        finally:
            self._state_stack.pop()

    def is_alive(self) -> bool:
        return bool(self._state_stack)


def format_error(error: StateError) -> str:
    return error.message


class GenericObserver:
    def __init__(
        self,
        state: State,
        logger: logging.Logger,
        cancellation: Cancellation,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self._state = state
        self._logger = logger
        self._cancellation = cancellation

    @contextmanager
    def observe(
        self,
        *,
        stage: str,
        critical: bool,
        exceptions: type | tuple[type, ...] | None = None,
        suppress: bool = True,
    ):
        exceptions = exceptions or Exception
        try:
            with self._state.enter_state(stage):
                yield
        except exceptions as e:
            msg = f'The error occured at "{stage}": {str(e)}'
            if critical:
                self._logger.error('The critical error occured: %s. Notifying service termination...', msg)
                self._cancellation.notify(msg)
            else:
                self._logger.info('Noncritical error: %s.', msg)
            self._state.error(stage, msg)

            if suppress:
                return True
            else:
                raise
        else:
            self._state.cleanup(stage)
