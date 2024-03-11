from typing import Optional, Dict, Protocol, List
from pydantic import BaseModel, Field
from ulta.common.config_validator import LabelKey, LabelValue

DEFAULT_ENVIRONMENT = 'DEFAULT'


class UltaConfig(BaseModel):
    command: str
    environment: str
    transport: str
    plugins: List[str] = Field(default_factory=list)
    netort_resource_manager: Optional[str] = None

    no_cache: bool = False

    backend_service_url: str
    iam_service_url: str
    logging_service_url: str
    object_storage_url: str

    agent_id_file: Optional[str] = None
    work_dir: str
    lock_dir: str
    request_frequency: int
    logging_path: Optional[str] = None
    logging_level: Optional[str] = None

    agent_name: Optional[str] = None
    folder_id: Optional[str] = None
    labels: Optional[Dict[LabelKey, LabelValue]] = None
    agent_version: Optional[str] = None
    service_account_key_path: Optional[str] = None
    service_account_id: Optional[str] = None
    service_account_key_id: Optional[str] = Field(default=None, json_schema_extra=dict(sensitive=True))
    service_account_private_key: Optional[str] = Field(default=None, json_schema_extra=dict(sensitive=True))
    oauth_token: Optional[str] = Field(default=None, json_schema_extra=dict(sensitive=True))
    iam_token: Optional[str] = Field(default=None, json_schema_extra=dict(sensitive=True))
    test_id: Optional[str] = None

    compute_instance_id: Optional[str] = None
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
