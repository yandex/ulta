import logging

from ulta.common.cancellation import Cancellation
from ulta.common.config import UltaConfig
from ulta.common.interfaces import NamedService, TransportFactory
from ulta.service.loadtesting_agent_service import (
    register_loadtesting_agent,
)
from ulta.service.artifact_uploader import S3ArtifactUploader
from ulta.service.log_uploader_service import LogUploaderService
from ulta.service.service import UltaService
from ulta.common.file_system import make_fs_from_ulta_config, FileSystemObserver
from ulta.common.healthcheck import HealthCheck
from ulta.common.state import State, GenericObserver
from ulta.service.status_reporter import StatusReporter, DummyStatusReporter
from ulta.service.tank_client import TankClient

MIN_SLEEP_TIME = 1


def run_serve(config: UltaConfig, cancellation: Cancellation, logger: logging.Logger) -> int:
    service_state = State()
    fs = make_fs_from_ulta_config(config)
    transport_factory = TransportFactory.get(config)
    observer = GenericObserver(service_state, logger, cancellation)

    agent = register_loadtesting_agent(config, transport_factory.create_agent_client(), observer, logger)
    loadtesting_client = transport_factory.create_loadtesting_client(agent)

    tank_client = TankClient(
        logger=logger,
        fs=fs,
        loadtesting_client=transport_factory.create_job_data_uploader_client(agent),
        data_uploader_api_address=config.backend_service_url,
    )

    sleep_time = max(config.request_frequency, MIN_SLEEP_TIME)
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
                LogUploaderService(transport_factory.create_cloud_logging_client(), cancellation, logger),
            ),
            NamedService(
                'S3 Artifact Uploader', S3ArtifactUploader(loadtesting_client, s3_client, cancellation, logger)
            ),
        ],
        cancellation=cancellation,
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
            sleep_time,
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
