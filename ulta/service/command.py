import contextlib
import logging
import multiprocessing
import signal
import typing
from ulta.common.agent import AgentInfo
from ulta.common.cancellation import Cancellation, CancellationType
from ulta.common.collections import QueueLike
from ulta.common.config import UltaConfig
from ulta.common.interfaces import ClientFactory, NamedService, TransportFactory
from ulta.common.logging import get_root_logger, get_logger, SinkHandler
from ulta.common.module import load_class
from ulta.service.artifact_uploader import S3ArtifactUploader
from ulta.service.loadtesting_agent_service import (
    register_loadtesting_agent,
)
from ulta.service.log_reporter import make_log_reporter, init_log_sink, release_log_sink, NullReporter
from ulta.service.log_uploader_service import LogUploaderService
from ulta.service.service import UltaService
from ulta.service.service_context import LabelContext
from ulta.common.file_system import make_fs_from_ulta_config, FileSystemObserver, FS
from ulta.common.healthcheck import HealthCheck
from ulta.common.state import State, GenericObserver
from ulta.common.utils import str_to_loglevel
from ulta.service.status_reporter import StatusReporter, DummyStatusReporter
from ulta.service.tank_client import TankClient, TankVariables
from yandextank.contrib.netort.netort.resource import ResourceManager, make_resource_manager

MIN_SLEEP_TIME = 1


def run_serve(config: UltaConfig, config_str: str, logger: logging.Logger) -> int:
    log_interceptor = init_log_interceptor(100_000)

    logger.info('Ulta service config %s', config_str)
    setup_plugins(config, logger)

    cancellation = setup_cancellation(logger)
    service_state = State()
    fs = make_fs_from_ulta_config(config)
    transport_factory = TransportFactory.get(config)
    observer = GenericObserver(service_state, logger, cancellation)

    agent = register_loadtesting_agent(config, transport_factory.create_agent_client(), observer, logger)
    label_context = LabelContext()
    with label_context.agent(agent):
        with run_log_reporters(
            config, agent, transport_factory, label_context, log_interceptor.sink
        ) as tank_logs_handler:
            release_log_interceptor(log_interceptor)
            additional_tank_log_handlers = []
            if tank_logs_handler is not None:
                additional_tank_log_handlers.append(tank_logs_handler)
            tank_client = TankClient(
                logger=logger,
                fs=fs,
                loadtesting_client=transport_factory.create_job_data_uploader_client(agent),
                data_uploader_api_address=config.backend_service_url,
                variables=_get_tank_variables(transport_factory, config),
                additional_tank_log_handlers=additional_tank_log_handlers,
            )
            return run_service(
                config=config,
                cancellation=cancellation,
                service_state=service_state,
                tank_client=tank_client,
                transport_factory=transport_factory,
                agent=agent,
                fs=fs,
                logger=logger,
                label_context=label_context,
            )


def run_service(
    *,
    config: UltaConfig,
    cancellation: Cancellation,
    service_state: State,
    tank_client: TankClient,
    transport_factory: ClientFactory,
    agent: AgentInfo,
    fs: FS,
    logger: logging.Logger,
    label_context: LabelContext,
) -> int:
    loadtesting_client = transport_factory.create_loadtesting_client(agent)

    sleep_time = max(config.request_interval, MIN_SLEEP_TIME)
    s3_client = transport_factory.create_s3_client()

    service = UltaService(
        logger=logger,
        state=service_state,
        loadtesting_client=loadtesting_client,
        tank_client=tank_client,
        s3_client=s3_client,
        tmp_dir=fs.tmp_dir,
        sleep_time=sleep_time,
        artifact_uploaders=[
            NamedService(
                'Cloud Logging uploader',
                LogUploaderService(transport_factory.create_logging_client(), cancellation, logger),
            ),
            NamedService(
                'S3 Artifact Uploader',
                S3ArtifactUploader(loadtesting_client, s3_client, cancellation, logger),
            ),
        ],
        cancellation=cancellation,
        label_context=label_context,
    )

    reporter_interval = (
        max(config.reporter_interval, MIN_SLEEP_TIME) if config.reporter_interval is not None else sleep_time
    )
    status_reporter = (
        DummyStatusReporter()
        if agent.is_anonymous_external_agent()
        else StatusReporter(
            logger,
            service,
            loadtesting_client,
            cancellation,
            service_state,
            reporter_interval,
        )
    )

    file_system_hc = FileSystemObserver(fs, service_state, logger, cancellation)
    observer = GenericObserver(service_state, logger, cancellation)
    with HealthCheck(observer, [file_system_hc]).run_healthcheck():
        with status_reporter.run():
            if config.test_id:
                result = service.serve_single_job(config.test_id)
                return result.exit_code
            else:
                service.serve()
                return 0 if service_state.ok else 1


def _get_tank_variables(transport_factory: ClientFactory, config: UltaConfig):
    cloud_token_getter: typing.Callable[[], str] | None = None
    if hasattr(transport_factory, 'get_iam_token'):
        token_getter_candidate = getattr(transport_factory, 'get_iam_token')
        if isinstance(token_getter_candidate, typing.Callable):
            cloud_token_getter = token_getter_candidate

    return TankVariables(
        token_getter=cloud_token_getter,
        aws_access_key_id=config.aws_access_key_id,
        aws_secret_access_key=config.aws_secret_access_key,
        s3_endpoint_url=config.object_storage_url,
    )


def init_log_interceptor(size: int) -> SinkHandler:
    logger = get_root_logger()
    handler = init_log_sink(None, size)
    logger.addHandler(handler)
    return handler


def release_log_interceptor(handler: SinkHandler):
    logger = get_root_logger()
    logger.removeHandler(handler)
    release_log_sink(handler)


@contextlib.contextmanager
def run_log_reporters(
    config: UltaConfig,
    agent: AgentInfo,
    transport_factory: ClientFactory,
    label_context: LabelContext,
    cached_logs: QueueLike | None,
):
    yandextank_events_level = str_to_loglevel(config.report_yandextank_log_events_level)
    tank_handler = None
    try:
        log_reporter = make_log_reporter(
            get_root_logger(),
            config,
            agent,
            transport_factory,
            label_context,
            cached_logs,
        )
        if not isinstance(log_reporter, NullReporter) and yandextank_events_level > logging.NOTSET:
            tank_log_sink = multiprocessing.Queue(100_000)
            tank_handler = init_log_sink(tank_log_sink, label_context=label_context)
            tank_handler.setLevel(yandextank_events_level)
            log_reporter.add_sources(tank_log_sink)

        with log_reporter.run():
            logger = get_logger()
            logger.info('Agent started')
            yield tank_handler
    finally:
        if tank_handler is not None:
            release_log_sink(tank_handler)


def setup_cancellation(logger: logging.Logger) -> Cancellation:
    cancellation = Cancellation()

    def terminate(signo, *args):
        if cancellation.is_set(CancellationType.FORCED):
            logger.warning('Received signal: %s. Terminating service', signal.Signals(signo).name)
        elif cancellation.is_set(CancellationType.GRACEFUL):
            cancellation.notify(f'Received signal: {signal.Signals(signo).name}', CancellationType.FORCED)
            logger.warning('Received duplicate signal: %s. Terminating...', signal.Signals(signo).name)
        else:
            cancellation.notify(f'Received signal: {signal.Signals(signo).name}', CancellationType.GRACEFUL)
            logger.warning(
                'Received signal: %s. Awaiting current job to finish and terminating...', signal.Signals(signo).name
            )

    signal.signal(signal.SIGINT, terminate)
    signal.signal(signal.SIGTERM, terminate)

    return cancellation


def setup_plugins(config: UltaConfig, logger: logging.Logger):
    # setup transport factory
    if config.transport:
        logger.info('Using transport factory %s', config.transport)
        TransportFactory.use(load_class(config.transport, base_class=ClientFactory))

    if config.netort_resource_manager:
        logger.info('Using netort resource manager %s', config.netort_resource_manager)
        resource_manager = load_class(config.netort_resource_manager, base_class=ResourceManager)
        TankClient.use_resource_manager(lambda *args: resource_manager())
    else:
        TankClient.use_resource_manager(lambda *args: make_resource_manager())
