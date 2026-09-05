import uuid
import logging
import pytest
from unittest.mock import patch, MagicMock
from google.protobuf.any_pb2 import Any

from ulta.common.agent import AgentInfo
from ulta.common.cancellation import Cancellation
from ulta.common.config import UltaConfig
from ulta.common.state import State, GenericObserver
from ulta.service.loadtesting_agent_service import (
    _identify_agent_id,
    register_loadtesting_agent,
    UNRECOVERABLE_REGISTRATION_DELAY,
    AgentOrigin,
    AgentOriginError,
)
import grpc
from ulta.yc.agent_client import (
    agent_registration_service_pb2,
    YCAgentClient,
)
from yandex.cloud.operation import operation_pb2


class Stb:
    Register = None
    ExternalAgentRegister = None


@pytest.fixture()
def patch_agent_registration_stub_register():
    with patch.object(Stb, 'Register') as p:
        yield p


@pytest.fixture()
def patch_agent_registration_stub_external_register():
    with patch.object(Stb, 'ExternalAgentRegister') as p:
        yield p


@pytest.fixture()
def patch_agent_registration_stub():
    with patch('ulta.yc.agent_client.agent_registration_service_pb2_grpc.AgentRegistrationServiceStub') as stb:
        stb.return_value = Stb
        yield stb


@pytest.mark.usefixtures('patch_agent_registration_stub')
def test_agent_send_version_on_greet(patch_agent_registration_stub_register):
    version = str(uuid.uuid4())
    patch_agent_registration_stub_register.return_value = agent_registration_service_pb2.RegisterResponse(
        agent_instance_id='abc'
    )

    agent_client = YCAgentClient(version, MagicMock(), MagicMock())
    agent = AgentInfo(
        id=None,
        name='some name',
        folder_id='folder_id',
        origin=AgentOrigin.COMPUTE_LT_CREATED,
        version=version,
    )
    agent_id = _identify_agent_id(agent, agent_client, logging.getLogger())

    assert agent_id == 'abc'
    patch_agent_registration_stub_register.assert_called_once()
    args, _ = patch_agent_registration_stub_register.call_args
    assert args[0].agent_version == version


@pytest.mark.usefixtures('patch_agent_registration_stub')
def test_external_agent_registration(patch_agent_registration_stub_external_register):
    version = str(uuid.uuid4())
    metadata = Any()
    metadata.Pack(agent_registration_service_pb2.ExternalAgentRegisterMetadata(agent_instance_id='abc-ext'))
    patch_agent_registration_stub_external_register.return_value = operation_pb2.Operation(metadata=metadata)

    token_provider = MagicMock()
    auth_metadata = ('authorization', 'some token')
    token_provider.get_auth_metadata.return_value = auth_metadata
    agent_client = YCAgentClient(version, MagicMock(), token_provider)
    agent = AgentInfo(
        id=None,
        name='agent_name',
        folder_id='folder_id',
        origin=AgentOrigin.EXTERNAL,
        version=version,
    )

    agent_id = _identify_agent_id(agent, agent_client, logging.getLogger())
    assert agent_id == 'abc-ext'
    patch_agent_registration_stub_external_register.assert_called_once()
    actual_request, kwargs = patch_agent_registration_stub_external_register.call_args
    assert actual_request[0].agent_version == version
    assert auth_metadata in kwargs['metadata']


@pytest.mark.usefixtures('patch_agent_registration_stub')
def test_external_agent_registration_fail():
    with pytest.raises(AgentOriginError):
        agent = AgentInfo(
            id=None,
            name='persistent',
            folder_id=None,
            origin=AgentOrigin.EXTERNAL,
            version=None,
        )
        _ = _identify_agent_id(agent, MagicMock(), logging.getLogger())


@pytest.mark.parametrize(
    'name, folder_id, is_anonymous, is_persistent',
    [
        (None, None, True, False),
        ('', '', True, False),
        ('some name', None, False, False),
        (None, 'folder_id', True, False),
        ('some name', 'folder_id', False, True),
    ],
)
def test_agent_state(name, folder_id, is_anonymous, is_persistent):
    agent = AgentInfo(
        id=None,
        name=name,
        version='1.0.0',
        origin=AgentOrigin.EXTERNAL,
        folder_id=folder_id,
    )

    assert agent.is_external() is True
    assert agent.is_anonymous_external_agent() == is_anonymous
    assert agent.is_persistent_external_agent() == is_persistent


class _Denied(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.PERMISSION_DENIED


def test_registration_denied_sleeps_before_exit():
    """Отказ по правам не должен превращаться в краш-луп: перед падением ждём (LOAD-3561)."""
    config = MagicMock(spec=UltaConfig)
    config.no_cache = True
    config.agent_id_file = None
    config.agent_name = 'persistent'
    config.folder_id = 'folder_id'
    config.instance_lt_created = False
    config.agent_version = '1.0'

    agent_client = MagicMock()
    agent_client.register_external_agent.side_effect = _Denied()
    observer = GenericObserver(State(), logging.getLogger(), Cancellation())

    with patch('time.sleep') as sleep:
        with pytest.raises(grpc.RpcError):
            register_loadtesting_agent(config, agent_client, observer, logging.getLogger())

    sleep.assert_called_once_with(UNRECOVERABLE_REGISTRATION_DELAY)
