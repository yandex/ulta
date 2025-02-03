from typing import TypeVar, Generic, Protocol, Callable, Any, runtime_checkable
from ulta.common.agent import AgentInfo
from ulta.common.config import UltaConfig

T = TypeVar('T')


class NamedService(Generic[T]):
    name: str
    service: T

    def __init__(self, name: str, service: T):
        self.name = name
        self.service = service


class LogMessage:
    def __init__(self, *, message: str, labels: dict[str, str], level: int, created_at: float):
        self.message = message
        self.level = level
        self.labels = labels
        self.created_at = created_at


class TankStatusClient(Protocol):
    def claim_tank_status(self, tank_status: str, status_message: str | None) -> None: ...

    def report_event_logs(self, idempotency_key: str, events: list[LogMessage]) -> None: ...


class JobFetcherClient(Protocol):
    def get_job(self, job_id: str | None = None): ...

    def download_transient_ammo(self, job_id, ammo_name, path_to_download) -> None: ...


class JobStatusClient(Protocol):
    def claim_job_status(self, job_id, job_status, error='', error_type=None) -> None: ...


class JobControlClient(Protocol):
    def get_job_signal(self, job_id): ...


class LoadtestingClient(TankStatusClient, JobFetcherClient, JobStatusClient, JobControlClient):
    pass


class JobDataUploaderClient(Protocol):
    def send_monitorings(self, job_id, data) -> None: ...

    def send_trails(self, job_id, trails) -> None: ...

    def set_imbalance_and_dsc(self, job_id, rps, timestamp, comment: str = '') -> None: ...

    def prepare_test_data(self, data_item, stat_item) -> Any: ...

    def prepare_monitoring_data(self, data_item) -> Any: ...


class AgentClient(Protocol):
    def register_agent(self) -> str: ...

    def register_external_agent(self, folder_id: str, name: str | None) -> str: ...


class RemoteLoggingClient(Protocol):
    def send_log(
        self,
        log_group_id: str,
        log_data,
        resource_type: str,
        resource_id: str,
        level=None,
        request_id=None,
        timeout=5.0,
    ) -> None: ...


class S3Client(Protocol):
    def download(self, storage_object, path_to_download) -> None: ...

    def upload(self, source_file, s3_filename, s3_bucket) -> None: ...


@runtime_checkable
class ClientFactory(Protocol):
    def create_agent_client(self) -> AgentClient: ...

    def create_loadtesting_client(self, agent: AgentInfo) -> LoadtestingClient: ...

    def create_job_data_uploader_client(self, agent: AgentInfo) -> LoadtestingClient: ...

    def create_s3_client(self) -> S3Client: ...

    def create_logging_client(self) -> RemoteLoggingClient: ...

    def create_events_log_client(self, agent: AgentInfo) -> RemoteLoggingClient: ...


class TransportFactory:
    _factory = None

    @classmethod
    def use(cls, factory: Callable[[UltaConfig], ClientFactory]):
        assert factory is not None
        cls._factory = factory

    @classmethod
    def get(cls, config: UltaConfig) -> ClientFactory:
        assert cls._factory is not None
        return cls._factory(config)
