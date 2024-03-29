from typing import Protocol
from pydantic import BaseModel, Field
from ulta.common.config_validator import LabelKey, LabelValue

DEFAULT_ENVIRONMENT = 'DEFAULT'


class UltaConfig(BaseModel):
    command: str
    environment: str
    transport: str
    plugins: list[str] = Field(default_factory=list)
    netort_resource_manager: str | None = None

    no_cache: bool = False

    backend_service_url: str
    iam_service_url: str
    logging_service_url: str
    object_storage_url: str

    agent_id_file: str | None = None
    work_dir: str
    lock_dir: str
    request_frequency: int
    logging_path: str | None = None
    logging_level: str | None = None

    agent_name: str | None = None
    folder_id: str | None = None
    labels: dict[LabelKey, LabelValue] | None = None
    agent_version: str | None = None
    service_account_key_path: str | None = None
    service_account_id: str | None = None
    service_account_key_id: str | None = Field(default=None, json_schema_extra=dict(sensitive=True))
    service_account_private_key: str | None = Field(default=None, json_schema_extra=dict(sensitive=True))
    oauth_token: str | None = Field(default=None, json_schema_extra=dict(sensitive=True))
    iam_token: str | None = Field(default=None, json_schema_extra=dict(sensitive=True))
    test_id: str | None = None

    compute_instance_id: str | None = None
    instance_lt_created: bool


class ExternalConfigLoader(Protocol):
    def name(self) -> str:
        ...

    def __call__(self, cfg: UltaConfig):
        ...

    @classmethod
    def create(cls) -> 'ExternalConfigLoader':
        return cls()

    @classmethod
    def env_type(cls) -> str:
        ...

    @classmethod
    def should_apply(cls, environment: str) -> bool:
        ...
