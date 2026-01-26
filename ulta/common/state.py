from contextlib import contextmanager
from datetime import datetime
from dataclasses import dataclass
from ulta.common.cancellation import Cancellation, CancellationRequest, CancellationType
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
        suppress: type[Exception] | tuple[type[Exception], ...] | None = None,
        error: type[Exception] | tuple[type[Exception], ...] | None = None,
        critical: type[Exception] | tuple[type[Exception], ...] | None = None,
    ):
        '''
        Context manager for exception handling with logging and state propagation.

        This helper provides context for exceptions and generic exception handling as follow

        All exceptions occurring within the context are logged.
        CancellationRequest exception is always raised

        If error matches `suppress` errors, it will not raise;
        If error matches `critical` errors, it will trigger cancellation.notify;
        If error matches `error` errors, it will be stored into state object;

        Prefer using observe(suppress=Exception) over `contextlib.suppress(Exception)` to properly handle CancellationRequest

        Example:
            >>> with self.observe(stage="processing", suppress=Exception, error=IOError):
            ...     # Suppress all exceptions and. Submit IOError to state object.
            ...     pass

            >>> with self.observe(stage="wait_for_task", critical=Unavailable, suppress=(TooManyRequests, ResourceExhausted)):
            ...     # Unavailable errors trigger cancellation request and graceful shutdown, ignore TooManyRequests and ResourceExhausted error
            ...     # all other errors should raise
            ...     pass
        '''

        noncritical_exceptions = GenericObserver._arg_to_type_tuple(suppress)
        error_exceptions = GenericObserver._arg_to_type_tuple(error)
        critical_exceptions = GenericObserver._arg_to_type_tuple(critical)

        try:
            self._state.cleanup(stage)
            self._cancellation.raise_on_set(CancellationType.FORCED)
            with self._state.enter_state(stage):
                yield
        except CancellationRequest:
            self._logger.warning('Terminating stage "%s" due to cancellation request.', stage)
            raise
        except Exception as e:
            msg = f'The error occured at "{stage}": {str(e)}'

            is_crit = isinstance(e, critical_exceptions)
            is_suppress = isinstance(e, noncritical_exceptions)

            if isinstance(e, error_exceptions):
                self._state.error(stage, msg)

            if is_crit:
                self._cancellation.notify(msg)
                self._logger.error('The critical error occured: %s. Notifying service termination...', msg)
                if is_suppress:
                    return

            if is_suppress:
                self._logger.info('Noncritical error occured at "%s": %s.', stage, str(e))
                return

            self._logger.error(msg)
            raise

    def _arg_to_type_tuple(arg: type[Exception] | tuple[type[Exception], ...] | None) -> tuple[type[Exception], ...]:
        if arg is None:
            return tuple()
        if isinstance(arg, tuple):
            return arg
        if isinstance(arg, (list, set)):
            return tuple(arg)
        return (arg,)
