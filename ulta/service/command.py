import logging
import os

from ulta.common.cancellation import Cancellation
from ulta.common.config import UltaConfig
from ulta.common.interfaces import NamedService, TransportFactory
from ulta.service.loadtesting_agent_service import create_loadtesting_agent_service
from ulta.service.artifact_uploader import S3ArtifactUploader
from ulta.service.log_uploader_service import LogUploaderService
from ulta.service.service import UltaService
from ulta.service.status_reporter import StatusReporter, DummyStatusReporter
from ulta.service.tank_client import TankClient

MIN_SLEEP_TIME = 1


def run_serve(config: UltaConfig, cancellation: Cancellation, logger: logging.Logger) -> int:
    transport_factory = TransportFactory.get(config)
    loadtesting_agent = create_loadtesting_agent_service(config, transport_factory.create_agent_client(), logger)
    agent = loadtesting_agent.register()
    if config.agent_id_file:
        os.makedirs(os.path.dirname(config.agent_id_file), exist_ok=True)
        loadtesting_agent.store_agent_id(agent)
    loadtesting_client = transport_factory.create_loadtesting_client(agent)

    tests_dir = os.path.join(config.work_dir, 'tests')
    os.makedirs(tests_dir, exist_ok=True)
    tank_client = TankClient(
        logger,
        tests_dir,
        config.lock_dir,
        transport_factory.create_job_data_uploader_client(agent),
        config.backend_service_url,
    )

    sleep_time = max(config.request_frequency, MIN_SLEEP_TIME)
    s3_client = transport_factory.create_s3_client()
    service_dir = os.path.join(config.work_dir, '_tmp')
    os.makedirs(service_dir, exist_ok=True)

    service = UltaService(
        logger=logger,
        loadtesting_client=loadtesting_client,
        tank_client=tank_client,
        s3_client=s3_client,
        work_dir=service_dir,
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
        else StatusReporter(logger, service, loadtesting_client, cancellation, sleep_time)
    )

    with status_reporter.run():
        if config.test_id:
            result = service.serve_single_job(config.test_id)
            return result.exit_code
        else:
            service.serve()
    return 0
