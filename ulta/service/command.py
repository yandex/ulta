import logging
import typing

from ulta.common.cancellation import Cancellation
from ulta.common.config import UltaConfig
from ulta.common.interfaces import ClientFactory, NamedService, TransportFactory
from ulta.service.artifact_uploader import S3ArtifactUploader
from ulta.service.loadtesting_agent_service import (
    register_loadtesting_agent,
)
from ulta.service.log_reporter import make_log_reporter
from ulta.service.log_uploader_service import LogUploaderService
from ulta.service.service import UltaService
from ulta.common.file_system import make_fs_from_ulta_config, FileSystemObserver
from ulta.common.healthcheck import HealthCheck
from ulta.common.state import State, GenericObserver
from ulta.service.status_reporter import StatusReporter, DummyStatusReporter
from ulta.service.tank_client import TankClient, TankVariables

MIN_SLEEP_TIME = 1


def run_serve(config: UltaConfig, cancellation: Cancellation, logger: logging.Logger) -> int:
    service_state = State()
    fs = make_fs_from_ulta_config(config)
    transport_factory = TransportFactory.get(config)
    observer = GenericObserver(service_state, logger, cancellation)

    agent = register_loadtesting_agent(config, transport_factory.create_agent_client(), observer, logger)
    with make_log_reporter(logger, config, agent, transport_factory).run():
        loadtesting_client = transport_factory.create_loadtesting_client(agent)

        tank_client = TankClient(
            logger=logger,
            fs=fs,
            loadtesting_client=transport_factory.create_job_data_uploader_client(agent),
            data_uploader_api_address=config.backend_service_url,
            variables=_get_tank_variables(transport_factory, config),
        )

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
