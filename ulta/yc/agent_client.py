import grpc
from ulta.common.utils import catch_exceptions
from ulta.common.interfaces import AgentClient
from ulta.yc.ycloud import METADATA_AGENT_VERSION_ATTR, TokenProviderProtocol
from yandex.cloud.loadtesting.agent.v1 import agent_registration_service_pb2, agent_registration_service_pb2_grpc


class YCAgentClient(AgentClient):
    def __init__(
        self,
        agent_version: str,
        grpc_channel: grpc.Channel,
        token_provider: TokenProviderProtocol,
        compute_instance_id: str | None = None,
        labels: str | None = None,
    ) -> None:
        self.timeout = 30.0
        self._token_provider = token_provider
        self.agent_version = agent_version
        self.compute_instance_id = compute_instance_id
        self._register_stub = agent_registration_service_pb2_grpc.AgentRegistrationServiceStub(grpc_channel)
        self.labels = labels

    def _request_metadata(self, agent_version: str):
        return [self._token_provider.get_auth_metadata(), (METADATA_AGENT_VERSION_ATTR, agent_version)]

    @catch_exceptions
    def register_agent(self) -> str:
        response = self._register_stub.Register(
            agent_registration_service_pb2.RegisterRequest(compute_instance_id=self.compute_instance_id),
            timeout=self.timeout,
            metadata=self._request_metadata(self.agent_version),
        )
        return response.agent_instance_id

    @catch_exceptions
    def register_external_agent(self, folder_id: str, name: str | None) -> str:
        response = self._register_stub.ExternalAgentRegister(
            agent_registration_service_pb2.ExternalAgentRegisterRequest(
                folder_id=folder_id,
                name=name,
                agent_version=self.agent_version,
            ),
            metadata=self._request_metadata(self.agent_version),
            timeout=self.timeout,
        )
        metadata = agent_registration_service_pb2.ExternalAgentRegisterMetadata()
        response.metadata.Unpack(metadata)
        return metadata.agent_instance_id
