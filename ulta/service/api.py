import asyncio
import logging
import threading
import json
from contextlib import contextmanager
from aiohttp import web
from strenum import StrEnum
from ulta.common.cancellation import Cancellation, CancellationType
from ulta.common.state import State


@contextmanager
def state_api(service_state: State, cancellation: Cancellation, port: int, logger: logging.Logger):
    started = threading.Event()
    stop = threading.Event()
    app = StateApp(service_state, cancellation)

    web_app = web.Application()
    web_app.add_routes([web.get('/health', app.get_health)])
    web_app.add_routes([web.get('/shutdown', app.shutdown)])
    web_app.add_routes([web.post('/shutdown', app.shutdown)])

    def entrypoint():
        asyncio.run(run_app_async(web_app, port, started, stop, logger))

    t = threading.Thread(target=entrypoint, name='ulta_state_api')
    t.start()
    while t.is_alive() and not started.is_set():
        started.wait(1)
    try:
        yield
    finally:
        stop.set()
        t.join()


async def run_app_async(
    app: web.Application, port: int, started: threading.Event, stop: threading.Event, logger: logging.Logger
):
    runner = web.AppRunner(app)
    await runner.setup()

    sites = make_sites(runner, port, ipv6=True, ipv4=True)
    if not sites:
        raise Exception(f'failed to start listeners on port {port}')

    started_sites = []
    for site in sites:
        try:
            await site.start()
            started_sites.append(site)
        except Exception:
            logger.warning('failed to start listener at %s', site.name)

    if not started_sites:
        raise Exception('failed to start aux app')

    started.set()

    logger.info('listeners %s started', [s.name for s in sites])
    while not stop.is_set():
        await asyncio.sleep(1)

    for site in started_sites:
        await site.stop()
    await runner.shutdown()


def make_sites(runner: web.AppRunner, port: int, ipv4: bool, ipv6: bool) -> list[web.TCPSite]:
    sites = []

    if ipv4:
        sites.append(web.TCPSite(runner, '127.0.0.1', port))

    if ipv6:
        sites.append(web.TCPSite(runner, '::1', port))

    return sites


class ServiceState(StrEnum):
    ALIVE = 'ALIVE'
    SHUTTING_DOWN = 'SHUTTING_DOWN'
    SHUTDOWN = 'SHUTDOWN'


class StateApp:
    def __init__(self, state: State, cancellation: Cancellation):
        self.state = state
        self.cancellation = cancellation

    async def get_health(self, request: web.Request):
        result = {
            'state': self._service_state(),
            'errors': self._errors(),
            'current_activity': self._current_activity(),
        }
        return web.Response(text=json.dumps(result))

    async def shutdown(self, request: web.Request):
        forced = request.query.get('force')
        self.cancellation.notify('requested from api', CancellationType.FORCED if forced else CancellationType.GRACEFUL)
        state = self._service_state()
        if state == ServiceState.SHUTDOWN:
            return web.Response(status=200, text=state)
        return web.Response(status=102, text=state)

    def _service_state(self):
        if not self.state.is_alive():
            return ServiceState.SHUTDOWN
        if self.cancellation.is_set(CancellationType.GRACEFUL):
            return ServiceState.SHUTTING_DOWN
        return ServiceState.ALIVE

    def _errors(self) -> list[str]:
        return [e.message for e in self.state.current_errors()]

    def _current_activity(self) -> str:
        state = ' -> '.join([s for s in self.state.current_state()])
        if not state:
            state = 'idle'
        return state
