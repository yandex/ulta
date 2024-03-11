import grpc

from typing import Optional
from yandex.cloud.loadtesting.agent.v1 import (
    agent_service_pb2,
    agent_service_pb2_grpc,
    job_service_pb2,
    job_service_pb2_grpc,
    monitoring_service_pb2,
    monitoring_service_pb2_grpc,
    test_service_pb2,
    test_service_pb2_grpc,
    trail_service_pb2,
    trail_service_pb2_grpc,
)

from ulta.common.agent import AgentInfo
from ulta.common.utils import catch_exceptions
from ulta.yc.ycloud import METADATA_AGENT_VERSION_ATTR, TokenProviderProtocol
from ulta.yc.trail_helper import prepare_trail_data, prepare_monitoring_data


class YCLoadtestingClient:
    def __init__(self, channel, token_provider: TokenProviderProtocol, agent: AgentInfo):
        self.timeout = 30.0
        self._token_provider = token_provider
        self.agent_id = agent.id
        self.agent_version = agent.version
        self.stub_agent = agent_service_pb2_grpc.AgentServiceStub(channel)
        self.stub_job = job_service_pb2_grpc.JobServiceStub(channel)

    def _request_metadata(self, additional_meta=None):
        meta = [self._token_provider.get_auth_metadata(), (METADATA_AGENT_VERSION_ATTR, self.agent_version)]
        if additional_meta:
            meta.extend(additional_meta)
        return meta

    @catch_exceptions
    def claim_tank_status(self, tank_status: str):
        # TODO return status and error message
        request = agent_service_pb2.ClaimAgentStatusRequest(agent_instance_id=self.agent_id, status=tank_status)
        result = self.stub_agent.ClaimStatus(request, timeout=self.timeout, metadata=self._request_metadata())
        return result.code

    @catch_exceptions
    def claim_job_status(self, job_id, job_status, error='', error_type=None):
        request = job_service_pb2.ClaimJobStatusRequest(job_id=job_id, status=job_status, error=error)
        metadata = []
        if error_type is not None:
            metadata.append(('error-type', error_type))
        result = self.stub_job.ClaimStatus(request, timeout=self.timeout, metadata=self._request_metadata(metadata))
        return result.code

    @catch_exceptions
    def get_job(self, job_id: Optional[str] = None) -> Optional[job_service_pb2.Job]:
        request = job_service_pb2.GetJobRequest(agent_instance_id=self.agent_id, job_id=job_id)

        try:
            job = self.stub_job.Get(
                request,
                timeout=self.timeout,
                metadata=self._request_metadata(),
            )
            return job
        except grpc.RpcError as error:
            if error.code() is grpc.StatusCode.NOT_FOUND:
                return None
            raise

    @catch_exceptions
    def get_job_signal(self, job_id):
        request = job_service_pb2.JobSignalRequest(job_id=job_id)
        result = self.stub_job.GetSignal(
            request,
            timeout=self.timeout,
            metadata=self._request_metadata(),
        )
        return result

    @catch_exceptions
    def download_transient_ammo(self, job_id, ammo_name, path_to_download):
        request = job_service_pb2.GetJobTransientFile(job_id=job_id, name=ammo_name)
        result = self.stub_job.GetTransientFile(
            request,
            timeout=self.timeout,
            metadata=self._request_metadata(),
        )
        with open(path_to_download, 'wb') as file:
            file.write(result.content)


class YCJobDataUploaderClient:
    def __init__(self, channel, token_provider: TokenProviderProtocol, agent: AgentInfo):
        self.timeout = 30.0
        self._token_provider = token_provider
        self.agent_id = agent.id
        self.agent_version = agent.version
        self.stub_monitoring = monitoring_service_pb2_grpc.MonitoringServiceStub(channel)
        self.stub_trail = trail_service_pb2_grpc.TrailServiceStub(channel)
        self.stub_test = test_service_pb2_grpc.TestServiceStub(channel)

    def _request_metadata(self, additional_meta=None):
        meta = [self._token_provider.get_auth_metadata(), (METADATA_AGENT_VERSION_ATTR, self.agent_version)]
        if additional_meta:
            meta.extend(additional_meta)
        return meta

    @catch_exceptions
    def send_monitorings(self, job_id, data):
        request = monitoring_service_pb2.AddMetricRequest(
            agent_instance_id=self.agent_id, job_id=str(job_id), chunks=data
        )
        result = self.stub_monitoring.AddMetric(
            request,
            timeout=self.timeout,
            metadata=self._request_metadata(),
        )
        return result.code

    @catch_exceptions
    def send_trails(self, job_id, trails):
        request = trail_service_pb2.CreateTrailRequest(agent_instance_id=self.agent_id, job_id=str(job_id), data=trails)
        result = self.stub_trail.Create(
            request,
            timeout=self.timeout,
            metadata=self._request_metadata(),
        )
        return result.code

    @catch_exceptions
    def set_imbalance_and_dsc(self, job_id, rps, timestamp, comment: str = ''):
        request = test_service_pb2.UpdateTestRequest(
            test_id=str(job_id),
            imbalance_point=rps,
            imbalance_ts=timestamp,
            imbalance_comment=comment,
        )
        self.stub_test.Update(request, timeout=self.timeout, metadata=self._request_metadata())

    def prepare_test_data(self, data_item, stat_item):
        return prepare_trail_data(trail_service_pb2, data_item, stat_item)

    def prepare_monitoring_data(self, data_item):
        return prepare_monitoring_data(monitoring_service_pb2, data_item)
