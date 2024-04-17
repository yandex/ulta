import logging
import pytest
from ulta.common.cancellation import Cancellation
from ulta.common.state import State, GenericObserver


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


def test_observer_default_catch_exception():
    cancellation = Cancellation()
    state = State()
    o = GenericObserver(state, logging.getLogger(), cancellation)
    with o.observe(stage='somethin important here'):
        raise Exception('first exception')
    assert state.ok is False
    assert cancellation.is_set() is True

    with o.observe(stage='somethin important here'):
        pass
    assert state.ok is True
    assert cancellation.is_set() is True


def test_observer_default_catch_custom_exception():
    cancellation = Cancellation()
    state = State()
    o = GenericObserver(state, logging.getLogger(), cancellation)
    with o.observe(stage='somethin important here', exceptions=OSError):
        raise FileNotFoundError('first exception')
    assert state.ok is False
    assert cancellation.is_set() is True
    with pytest.raises(Exception, match='second exception'):
        with o.observe(stage='somethin very important here', exceptions=OSError):
            raise Exception('second exception')


def test_observer_do_not_cancel_on_noncrit():
    cancellation = Cancellation()
    state = State()
    o = GenericObserver(state, logging.getLogger(), cancellation)
    with o.observe(stage='somethin noncritical here', critical=False):
        raise Exception('first exception')
    assert state.ok is False
    assert cancellation.is_set() is False

    with o.observe(stage='somethin noncritical here', critical=False):
        pass
    assert state.ok is True
    assert cancellation.is_set() is False
