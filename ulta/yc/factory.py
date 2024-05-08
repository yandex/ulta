import grpc

from functools import lru_cache

from ulta.common.agent import AgentInfo
from ulta.common.config import UltaConfig
from ulta.common.interfaces import ClientFactory
from ulta.yc.agent_client import YCAgentClient
from ulta.yc.backend_client import YCLoadtestingClient, YCJobDataUploaderClient
from ulta.yc.config import YANDEX_COMPUTE
from ulta.yc.cloud_logging_client import YCCloudLoggingClient
from ulta.yc.s3_client import YCS3Client
from ulta.yc.ycloud import (
    AuthTokenProvider,
    build_sa_key,
    create_cloud_channel,
    AUDIENCE_URL_FROM_IAM_ENDPOINT,
)


class YCFactory(ClientFactory):
    def __init__(self, config: UltaConfig) -> None:
        sa_key = build_sa_key(
            sa_key=config.service_account_private_key,
            sa_key_file=config.service_account_key_path,
            sa_key_id=config.service_account_key_id,
            sa_id=config.service_account_id,
        )
        self.token_provider = AuthTokenProvider(
            iam_endpoint=config.iam_service_url,
            iam_token=config.iam_token,
            sa_key=sa_key,
            oauth_token=config.oauth_token,
            use_metadata_token=use_compute_metadata(config),
            audience_url=AUDIENCE_URL_FROM_IAM_ENDPOINT,
        )
        self.channels = ChannelFactory()
        self.config = config

    def get_iam_token(self) -> str:
        return self.token_provider.get_token()

    def create_agent_client(self) -> YCAgentClient:
        return YCAgentClient(
            self.config.agent_version,
            self.channels.get_channel(self.config.backend_service_url),
            self.token_provider,
            self.config.compute_instance_id,
            self.config.labels,
        )

    def create_loadtesting_client(self, agent: AgentInfo) -> YCLoadtestingClient:
        return YCLoadtestingClient(
            self.channels.get_channel(self.config.backend_service_url), self.token_provider, agent
        )

    def create_job_data_uploader_client(self, agent: AgentInfo) -> YCLoadtestingClient:
        return YCJobDataUploaderClient(
            self.channels.get_channel(self.config.backend_service_url), self.token_provider, agent
        )

    def create_s3_client(self) -> YCS3Client:
        return YCS3Client(
            self.config.object_storage_url,
            self.token_provider,
        )

    def create_cloud_logging_client(self) -> YCCloudLoggingClient:
        return YCCloudLoggingClient(self.channels.get_channel(self.config.logging_service_url), self.token_provider)


class ChannelFactory:
    @lru_cache
    def get_channel(self, url: str, channel_options=None, insecure_connection=False) -> grpc.Channel:
        return create_cloud_channel(url, insecure_connection, channel_options)


def use_compute_metadata(config: UltaConfig) -> bool:
    return config.environment == YANDEX_COMPUTE
