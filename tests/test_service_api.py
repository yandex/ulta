import logging
import pytest
import requests
import socket
from ulta.common.cancellation import Cancellation
from ulta.common.state import State, GenericObserver
from ulta.service.api import state_api


@pytest.fixture
def free_port():
    sock = socket.socket()
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    yield port


def test_state_service_api_handlers(free_port):
    s = State()
    cancellation = Cancellation()
    logger = logging.getLogger('test')
    port = free_port
    with state_api(s, cancellation, port, logger):
        uri = f'http://127.0.0.1:{port}'
        health_response = requests.get(f'{uri}/health')
        shutdown_response = requests.get(f'{uri}/shutdown')

    assert health_response.text == '{"state": "SHUTDOWN", "errors": [], "current_activity": "idle"}'
    assert shutdown_response.text == 'SHUTDOWN'


def test_state_service_api_handlers_with_state(free_port):
    s = State()
    cancellation = Cancellation()
    logger = logging.getLogger('test')
    port = free_port
    uri = f'http://127.0.0.1:{port}'
    with state_api(s, cancellation, port, logger):
        with GenericObserver(s, logger, cancellation).observe(stage='testing 123', error=Exception):
            try:
                with GenericObserver(s, logger, cancellation).observe(stage='sub_stage', error=Exception):
                    raise Exception('something went wrong')
            except Exception:
                pass

            health_response = requests.get(f'{uri}/health')
            _ = requests.get(f'{uri}/shutdown')

    assert (
        health_response.text
        == '{"state": "ALIVE", "errors": ["The error occured at \\"sub_stage\\": something went wrong"], "current_activity": "testing 123"}'
    )


def test_state_service_api_shutdown(free_port):
    s = State()
    cancellation = Cancellation()
    logger = logging.getLogger('test')
    port = free_port
    uri = f'http://127.0.0.1:{port}'
    with state_api(s, cancellation, port, logger):
        with GenericObserver(s, logger, cancellation).observe(stage='testing 123'):
            with GenericObserver(s, logger, cancellation).observe(stage='sub_stage'):
                _ = requests.post(f'{uri}/shutdown')
                health_response = requests.get(f'{uri}/health')

        assert (
            health_response.text
            == '{"state": "SHUTTING_DOWN", "errors": [], "current_activity": "testing 123 -> sub_stage"}'
        )

        health_response = requests.get(f'{uri}/health')
        shutdown_response = requests.get(f'{uri}/shutdown')
        assert health_response.text == '{"state": "SHUTDOWN", "errors": [], "current_activity": "idle"}'
        assert shutdown_response.text == 'SHUTDOWN'
