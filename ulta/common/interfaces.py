from typing import Optional, TypeVar, Generic, Protocol, Callable, Any
from ulta.common.agent import AgentInfo
from ulta.common.config import UltaConfig

T = TypeVar('T')


class NamedService(Generic[T]):
    name: str
    service: T

    def __init__(self, name: str, service: T):
        self.name = name
        self.service = service


class TankStatusClient(Protocol):
    def claim_tank_status(self, tank_status: str) -> None:
        pass


class JobFetcherClient(Protocol):
    def get_job(self, job_id: Optional[str] = None):
        pass

    def download_transient_ammo(self, job_id, ammo_name, path_to_download) -> None:
        pass


class JobStatusClient(Protocol):
    def claim_job_status(self, job_id, job_status, error='', error_type=None) -> None:
        pass


class JobControlClient(Protocol):
    def get_job_signal(self, job_id):
        pass


class LoadtestingClient(TankStatusClient, JobFetcherClient, JobStatusClient, JobControlClient):
    pass


class JobDataUploaderClient(Protocol):
    def send_monitorings(self, job_id, data) -> None:
        pass

    def send_trails(self, job_id, trails) -> None:
        pass

    def set_imbalance_and_dsc(self, job_id, rps, timestamp, comment: str = '') -> None:
        pass

    def prepare_test_data(self, data_item, stat_item) -> Any:
        pass

    def prepare_monitoring_data(self, data_item) -> Any:
        pass


class AgentClient(Protocol):
    def register_agent(self, compute_instance_id: str) -> str:
        pass

    def register_external_agent(self, folder_id: str, name: str, agent_version: str) -> str:
        pass


class CloudLoggingClient(Protocol):
    def send_log(
        self, log_group_id, log_data, resource_type, resource_id, level=None, request_id=None, timeout=5.0
    ) -> None:
        pass


class S3Client(Protocol):
    def download(self, storage_object, path_to_download) -> None:
        pass

    def upload(self, source_file, s3_filename, s3_bucket) -> None:
        pass


class ClientFactory(Protocol):
    def create_agent_client(self) -> AgentClient:
        pass

    def create_loadtesting_client(self, agent: AgentInfo) -> LoadtestingClient:
        pass

    def create_job_data_uploader_client(self, agent: AgentInfo) -> LoadtestingClient:
        pass

    def create_s3_client(self) -> S3Client:
        pass

    def create_cloud_logging_client(self) -> CloudLoggingClient:
        pass


class TransportFactory:
    _factory = None

    @classmethod
    def use(cls, factory: Callable[[UltaConfig], ClientFactory]):
        assert factory is not None
        cls._factory = factory

    @classmethod
    def get(cls, config: UltaConfig) -> ClientFactory:
        return cls._factory(config)
