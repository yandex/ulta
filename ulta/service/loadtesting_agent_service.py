import logging

from pathlib import Path
from ulta.common.agent import AgentInfo, AgentOrigin
from ulta.common.config import UltaConfig
from ulta.common.file_system import ensure_dir
from ulta.common.interfaces import AgentClient
from ulta.common.state import GenericObserver

ANONYMOUS_AGENT_ID = None


class AgentOriginError(Exception):
    pass


def register_loadtesting_agent(
    config: UltaConfig,
    agent_client: AgentClient,
    observer: GenericObserver,
    logger: logging.Logger,
):
    agent = make_agent_info_from_config(config)
    if agent.is_persistent_external_agent() and not config.no_cache and config.agent_id_file:
        with observer.observe(stage='load cached agent id from file'):
            agent.id = try_read_agent_id(config.agent_id_file, logger)

    with observer.observe(stage="register agent in service"):
        agent.id = agent.id or _identify_agent_id(agent, agent_client, logger)

    if not config.no_cache and config.agent_id_file and agent.is_persistent_external_agent() and agent.id:
        with observer.observe(stage='cache agent id to file'):
            try_store_agent_id(agent.id, config.agent_id_file)

    return agent


def _identify_agent_id(agent: AgentInfo, agent_client: AgentClient, logger: logging.Logger) -> str | None:
    if agent.origin is AgentOrigin.COMPUTE_LT_CREATED:
        agent_instance_id = agent_client.register_agent()
        logger.info('The agent has been registered with id "%(agent_id)s"', dict(agent_id=agent_instance_id))
        return agent_instance_id

    if agent.is_persistent_external_agent():
        agent_instance_id = agent_client.register_external_agent(folder_id=agent.folder_id, name=agent.name)
        logger.info('The agent has been registered with id "%(agent_id)s"', dict(agent_id=agent_instance_id))
        return agent_instance_id
    elif agent.is_anonymous_external_agent():
        logger.info('The agent is anonymous')
        return ANONYMOUS_AGENT_ID
    else:
        raise AgentOriginError(
            'Unable to identify agent id. If you running external agent ensure folder id and service account key are provided'
        )


def _identify_agent_origin(config: UltaConfig) -> AgentOrigin:
    if config.instance_lt_created and config.compute_instance_id:
        return AgentOrigin.COMPUTE_LT_CREATED
    return AgentOrigin.EXTERNAL


def make_agent_info_from_config(config: UltaConfig):
    return AgentInfo(
        id='',
        name=config.agent_name,
        version=config.agent_version,
        origin=_identify_agent_origin(config),
        folder_id=config.folder_id,
    )


def try_read_agent_id(agent_id_file: str | None, logger: logging.Logger) -> str | None:
    agent_id = None
    if not agent_id_file:
        return agent_id

    try:
        with open(agent_id_file, '+r') as f:
            agent_id = f.read(50)
    except FileNotFoundError as e:
        logger.error(
            'Failed to load agent_id from file %(file_name)s: %(error)s', dict(file_name=agent_id_file, error=str(e))
        )
        return None
    else:
        logger.info(
            'Load agent_id from file %(file_name)s: "%(agent_id)s"', dict(file_name=agent_id_file, agent_id=agent_id)
        )
    return agent_id


def try_store_agent_id(agent_id: str, agent_id_file: str):
    if not agent_id:
        return
    if not agent_id_file:
        raise ValueError('agent_id_file parameter must be set for store_agent_id')

    agent_id_path = Path(agent_id_file)
    ensure_dir(agent_id_path.parent)
    agent_id_path.write_text(agent_id)
