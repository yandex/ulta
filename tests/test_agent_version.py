import uuid
from unittest.mock import patch, MagicMock

import pytest

from ulta.common.agent import AgentInfo, AgentOrigin
from ulta.yc.backend_client import YCLoadtestingClient, METADATA_AGENT_VERSION_ATTR, job_service_pb2


@pytest.fixture()
def patch_job_stub():
    class Stb:
        Get = None

    with patch.object(Stb, 'Get') as sh:
        sh.return_value = job_service_pb2.Job(id='fake_id')
        with patch('ulta.yc.backend_client.job_service_pb2_grpc.JobServiceStub') as stb:
            stb.return_value = Stb
            yield Stb


def test_agent_send_version_on_get_job(patch_job_stub):
    version = str(uuid.uuid4())
    agent_info = AgentInfo(
        id='some_id',
        folder_id='some_folder_id',
        name='some_name',
        origin=AgentOrigin.UNKNOWN,
        version=version,
    )

    client = YCLoadtestingClient(MagicMock(), MagicMock(), agent_info)
    client.get_job()

    patch_job_stub.Get.assert_called_once()
    _, kwargs = patch_job_stub.Get.call_args
    assert 'metadata' in kwargs
    assert (METADATA_AGENT_VERSION_ATTR, version) in kwargs['metadata']
