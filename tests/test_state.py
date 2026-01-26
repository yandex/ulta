import logging
import pytest
from datetime import datetime
from ulta.common.cancellation import Cancellation, CancellationRequest, CancellationType
from ulta.common.state import State, GenericObserver, StateError


@pytest.mark.parametrize(
    'errors, expected_ok, expected_errors, expected_message',
    [
        ([], True, [], ''),
        ([Exception('generic exception')], False, ['generic exception'], 'generic exception'),
        (
            [PermissionError('read access denied to working folder'), FileNotFoundError('agent_id file not found')],
            False,
            ['read access denied to working folder', 'agent_id file not found'],
            'read access denied to working folder\nagent_id file not found',
        ),
    ],
)
def test_state(errors, expected_ok, expected_errors, expected_message):
    s = State()
    for e in errors:
        s.error('stage', e)
    assert s.ok == expected_ok
    assert [e.message for e in s.current_errors()] == expected_errors
    assert s.get_summary_message() == expected_message


def test_state_forgets_old_errors():
    s = State()
    s.error('stage', Exception('wow some old exception here'))
    s.error('stage 2', Exception('wow some another exception here'))
    assert s.ok is False
    assert len(s.current_errors()) == 2
    assert s.get_summary_message() == 'wow some old exception here\nwow some another exception here'

    s.cleanup('stage')
    assert len(s.current_errors()) == 1
    assert s.ok is False
    assert s.get_summary_message() == 'wow some another exception here'

    s.cleanup('stage 2')
    assert len(s.current_errors()) == 0
    assert s.ok is True
    assert s.get_summary_message() == ''


def test_state_can_handle_duplicate_errors():
    s = State()
    s.error('stage', Exception('wow some exception here'))
    s.error('stage', Exception('wow some exception here'))
    s.error('stage', Exception('wow some very new exception here'))
    s.error('stage', Exception('wow some exception here'))
    assert len(s.current_errors()) == 2
    assert s.ok is False
    assert s.get_summary_message() == 'wow some exception here\nwow some very new exception here'
    assert any(
        e for e in s.current_errors() if e.message == 'wow some exception here' and e.stage == 'stage'
    ), 'Error with message "wow some exception here" not found in state.current_errors()'
    assert any(
        e for e in s.current_errors() if e.message == 'wow some very new exception here' and e.stage == 'stage'
    ), 'Error with message "wow some very new exception here" not found in state.current_errors()'


def assertStateErrors(state: State, errs: StateError | list[StateError]):
    if isinstance(errs, StateError):
        errs = [errs]
    if len(state.current_errors()) != len(errs):
        # just getting nice message here
        assert state == errs
        return

    for i in range(len(errs)):
        assert state.current_errors()[i].message == errs[i].message
        assert state.current_errors()[i].stage == errs[i].stage
        assert state.current_errors()[i].updated_at.timestamp() > 0


def test_observer_raises_exception_and_request_cancellation_on_critical():
    cancellation = Cancellation()
    state = State()
    o = GenericObserver(state, logging.getLogger(), cancellation)
    with pytest.raises(ValueError):
        with o.observe(stage='somethin important here', critical=Exception, error=Exception):
            raise ValueError('first exception')
    assert state.ok is False
    assert cancellation.is_set() is True
    assertStateErrors(
        state,
        StateError(
            datetime.now(), 'somethin important here', 'The error occured at "somethin important here": first exception'
        ),
    )

    with o.observe(stage='somethin important there', critical=Exception):
        pass
    # check that nothing changes
    assert state.ok is False
    assert cancellation.is_set() is True
    assertStateErrors(
        state,
        StateError(
            datetime.now(), 'somethin important here', 'The error occured at "somethin important here": first exception'
        ),
    )


def test_observer_suppress_exception():
    cancellation = Cancellation()
    state = State()
    o = GenericObserver(state, logging.getLogger(), cancellation)
    with o.observe(stage='absolutely non imporant block', suppress=Exception):
        raise ValueError('this is non important error')
    assert state.ok is True
    assert cancellation.is_set() is False
    assert not state.current_errors()


def test_observer_raise_cancellation_request_before_enter_to_context():
    cancellation = Cancellation()
    cancellation.notify('test', level=CancellationType.FORCED)
    state = State()
    o = GenericObserver(state, logging.getLogger(), cancellation)
    with pytest.raises(CancellationRequest):
        with o.observe(stage='absolutely non imporant block', suppress=Exception):
            raise ValueError('this should never happen')
    assert state.ok is True
    assert not state.current_errors()


def test_observer_suppress_error_even_if_its_critical():
    cancellation = Cancellation()
    state = State()
    o = GenericObserver(state, logging.getLogger(), cancellation)
    with o.observe(stage='important block, but some errors are ok', critical=OSError, suppress=FileNotFoundError):
        raise FileNotFoundError('its fine')
    assert state.ok is True
    assert cancellation.is_set() is True
    assert not state.current_errors()


def test_observer_raises_nonsuppressed_exception():
    cancellation = Cancellation()
    state = State()
    o = GenericObserver(state, logging.getLogger(), cancellation)
    with pytest.raises(ArithmeticError):
        with o.observe(stage='runtime error', critical=RuntimeError, suppress=ValueError):
            raise ArithmeticError('what happened')
    assert state.ok is True
    assert cancellation.is_set() is False
    assert not state.current_errors()


def test_observer_handle_exception_tuples():
    cancellation = Cancellation()
    state = State()
    o = GenericObserver(state, logging.getLogger(), cancellation)
    with o.observe(stage='ok', critical=(RuntimeError, OSError), suppress=(ValueError, KeyError)):
        raise KeyError('key error')
    assert state.ok is True
    assert cancellation.is_set() is False
    assert not state.current_errors()

    with o.observe(
        stage='runtime error', critical=(RuntimeError, OSError), suppress=(ValueError, KeyError, FileNotFoundError)
    ):
        raise FileNotFoundError('file not found')
    assert state.ok is True
    assert cancellation.is_set() is True
    assert not state.current_errors()


def test_observer_cleanup_stage_error_on_enter():
    cancellation = Cancellation()
    state = State()
    state.error('stage', 'old error')
    o = GenericObserver(state, logging.getLogger(), cancellation)

    with o.observe(stage='stage', critical=(RuntimeError, OSError), suppress=(ValueError, KeyError)):
        assert state.ok is True
        assert not state.current_errors()


def test_observer_do_not_cleanup_other_stage_errors():
    cancellation = Cancellation()
    state = State()
    state.error('other_stage', 'old error')
    o = GenericObserver(state, logging.getLogger(), cancellation)

    with o.observe(stage='stage', critical=(RuntimeError, OSError), suppress=(ValueError, KeyError)):
        pass

    assert state.ok is False
    assertStateErrors(state, StateError(datetime.now(), 'other_stage', 'old error'))


def test_observer_writes_error_to_state():
    cancellation = Cancellation()
    state = State()
    o = GenericObserver(state, logging.getLogger(), cancellation)
    with pytest.raises(ArithmeticError):
        with o.observe(stage='runtime error', error=Exception):
            raise ArithmeticError('what happened')
    assert state.ok is False
    assert cancellation.is_set() is False
    assertStateErrors(
        state, StateError(datetime.now(), 'runtime error', 'The error occured at "runtime error": what happened')
    )


def test_observer_writes_suppressed_error_to_state():
    cancellation = Cancellation()
    state = State()
    o = GenericObserver(state, logging.getLogger(), cancellation)
    with o.observe(stage='runtime error', error=Exception, suppress=ArithmeticError):
        raise ArithmeticError('what happened')
    assert state.ok is False
    assert cancellation.is_set() is False
    assertStateErrors(
        state, StateError(datetime.now(), 'runtime error', 'The error occured at "runtime error": what happened')
    )


def test_observer_writes_critial_error_to_state():
    cancellation = Cancellation()
    state = State()
    o = GenericObserver(state, logging.getLogger(), cancellation)
    with pytest.raises(ArithmeticError):
        with o.observe(stage='runtime error', error=Exception, critical=ArithmeticError):
            raise ArithmeticError('what happened')
    assert state.ok is False
    assert cancellation.is_set() is True
    assertStateErrors(
        state, StateError(datetime.now(), 'runtime error', 'The error occured at "runtime error": what happened')
    )


def test_observer_writes_exact_error_to_state():
    cancellation = Cancellation()
    state = State()
    o = GenericObserver(state, logging.getLogger(), cancellation)
    with pytest.raises(Exception):
        with o.observe(stage='runtime error', error=Exception, critical=ArithmeticError):
            raise Exception('what happened')
    assert state.ok is False
    assert cancellation.is_set() is False
    assertStateErrors(
        state, StateError(datetime.now(), 'runtime error', 'The error occured at "runtime error": what happened')
    )
