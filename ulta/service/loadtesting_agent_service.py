import logging

from ulta.common.agent import AgentInfo, AgentOrigin
from ulta.common.config import UltaConfig
from ulta.common.interfaces import AgentClient

ANONYMOUS_AGENT_ID = None


class AgentOriginError(Exception):
    pass


class LoadtestingAgentService(object):
    def __init__(
        self,
        logger: logging.Logger,
        agent_client: AgentClient,
        agent_origin: AgentOrigin | None = None,
        agent_id: str | None = None,
        agent_id_file: str | None = None,
        agent_name: str | None = None,
        agent_version: str | None = None,
        folder_id: str | None = None,
        compute_instance_id: str | None = None,
        instance_lt_created: bool = False,
        use_cached_agent_id: bool = True,
    ):
        self.logger = logger
        self.agent_client = agent_client
        self.compute_instance_id = compute_instance_id
        self.instance_lt_created = bool(instance_lt_created)
        self.agent_id_file = agent_id_file
        self.folder_id = folder_id
        self.agent = AgentInfo(
            id=agent_id,
            name=agent_name,
            version=agent_version,
            origin=agent_origin or self._identify_agent_origin(),
            folder_id=folder_id,
        )
        self._agent_registered = False
        self.use_cached_agent_id = use_cached_agent_id

    def register(self) -> AgentInfo:
        if not self._agent_registered:
            self.agent.id = self.agent.id or self._identify_agent_id()
            self._agent_registered = True
        return self.agent

    def _identify_agent_origin(self) -> AgentOrigin:
        if self.instance_lt_created and self.compute_instance_id:
            return AgentOrigin.COMPUTE_LT_CREATED
        return AgentOrigin.EXTERNAL

    def _identify_agent_id(self) -> str | None:
        if self.agent.origin is AgentOrigin.COMPUTE_LT_CREATED:
            agent_instance_id = self.agent_client.register_agent()
            self.logger.info('The agent has been registered with id(%s)', agent_instance_id)
            return agent_instance_id

        if self.use_cached_agent_id and (agent_id := self._load_agent_id()):
            self.logger.info('Load agent_id from file (%s)', agent_id)
            return agent_id
        elif self.agent.is_persistent_external_agent():
            agent_instance_id = self.agent_client.register_external_agent(
                folder_id=self.folder_id, name=self.agent.name
            )
            self.logger.info('The agent has been registered with id(%s)', agent_instance_id)
            return agent_instance_id
        elif self.agent.is_anonymous_external_agent():
            return ANONYMOUS_AGENT_ID
        else:
            raise AgentOriginError(
                'Unable to identify agent id. If you running external agent ensure folder id and service account key are provided'
            )

    def store_agent_id(self, agent: AgentInfo):
        if not agent.id:
            return
        if not self.agent_id_file:
            raise ValueError('agent_id_file parameter must be set for store_agent_id')
        try:
            with open(self.agent_id_file, 'w') as f:
                f.write(agent.id)
        except Exception as e:
            self.logger.error('Failed to save agent_id to file %s: %s', self.agent_id_file, e)

    def _load_agent_id(self) -> str:
        if self.agent_id_file:
            try:
                with open(self.agent_id_file, '+r') as f:
                    return f.read(50)
            except FileNotFoundError as e:
                self.logger.error('Failed to load agent_id from file %s: %s', self.agent_id_file, e)

        return ''


def create_loadtesting_agent_service(
    config: UltaConfig, agent_client: AgentClient, logger: logging.Logger
) -> LoadtestingAgentService:
    return LoadtestingAgentService(
        logger,
        agent_client,
        agent_id_file=config.agent_id_file,
        agent_name=config.agent_name,
        folder_id=config.folder_id,
        compute_instance_id=config.compute_instance_id,
        agent_version=config.agent_version,
        instance_lt_created=config.instance_lt_created,
        use_cached_agent_id=(not config.no_cache),
    )
