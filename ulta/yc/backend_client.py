import grpc
import logging

from google.protobuf.field_mask_pb2 import FieldMask
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
from ulta.common.interfaces import RemoteLoggingClient, LogMessage
from ulta.common.utils import catch_exceptions, retry_lt_client_call, float_to_proto_timestamp
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
    @retry_lt_client_call
    def claim_tank_status(self, tank_status: str, status_message: str | None):
        # TODO return status and error message
        request = agent_service_pb2.ClaimAgentStatusRequest(
            agent_instance_id=self.agent_id, status=tank_status, status_message=status_message
        )
        result = self.stub_agent.ClaimStatus(request, timeout=self.timeout, metadata=self._request_metadata())
        return result.code

    @catch_exceptions
    @retry_lt_client_call
    def report_event_logs(self, idempotency_key: str, events: list[LogMessage]) -> None:
        return None

    @catch_exceptions
    @retry_lt_client_call
    def claim_job_status(self, job_id, job_status, error='', error_type=None):
        request = job_service_pb2.ClaimJobStatusRequest(job_id=job_id, status=job_status, error=error)
        metadata = []
        if error_type is not None:
            metadata.append(('error-type', error_type))
        result = self.stub_job.ClaimStatus(request, timeout=self.timeout, metadata=self._request_metadata(metadata))
        return result.code

    @catch_exceptions
    @retry_lt_client_call
    def get_job(self, job_id: str | None = None) -> job_service_pb2.Job | None:
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
    @retry_lt_client_call
    def get_job_signal(self, job_id):
        request = job_service_pb2.JobSignalRequest(job_id=job_id)
        result = self.stub_job.GetSignal(
            request,
            timeout=self.timeout,
            metadata=self._request_metadata(),
        )
        return result

    @catch_exceptions
    @retry_lt_client_call
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
    @retry_lt_client_call
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
    @retry_lt_client_call
    def send_trails(self, job_id, trails):
        request = trail_service_pb2.CreateTrailRequest(agent_instance_id=self.agent_id, job_id=str(job_id), data=trails)
        result = self.stub_trail.Create(
            request,
            timeout=self.timeout,
            metadata=self._request_metadata(),
        )
        return result.code

    @catch_exceptions
    @retry_lt_client_call
    def set_imbalance_and_dsc(self, job_id, rps, timestamp, comment: str = ''):
        request = test_service_pb2.UpdateTestRequest(
            test_id=str(job_id),
            imbalance_point=rps,
            imbalance_ts=timestamp,
            imbalance_comment=comment,
            update_mask=self._make_update_mask_for_set_imbalance(timestamp),
        )
        self.stub_test.Update(request, timeout=self.timeout, metadata=self._request_metadata())

    def _make_update_mask_for_set_imbalance(self, timestamp: int) -> FieldMask:
        paths = ['imbalance_point', 'imbalance_comment']
        if timestamp:
            paths.append('imbalance_ts')
        return FieldMask(paths=paths)

    def prepare_test_data(self, data_item, stat_item):
        return prepare_trail_data(trail_service_pb2, data_item, stat_item)

    def prepare_monitoring_data(self, data_item):
        return prepare_monitoring_data(monitoring_service_pb2, data_item)


class YCEventLogClient(RemoteLoggingClient):
    def __init__(self, channel, token_provider: TokenProviderProtocol, agent: AgentInfo):
        self.timeout = 10.0
        self._token_provider = token_provider
        self.agent_id = agent.id
        self.stub_agent = agent_service_pb2_grpc.AgentServiceStub(channel)

    def _request_metadata(self):
        return [self._token_provider.get_auth_metadata()]

    def send_log(
        self,
        log_group_id: str,
        log_data: list[LogMessage],
        resource_type: str,
        resource_id: str,
        level=None,
        request_id=None,
        timeout=5.0,
    ) -> None:
        return self.report_event_logs(idempotency_key=request_id, events=log_data)

    @catch_exceptions
    @retry_lt_client_call
    def report_event_logs(self, idempotency_key: str, events: list[LogMessage]):
        request = agent_service_pb2.ReportEventLogsRequest(
            agent_instance_id=str(self.agent_id),
            idempotency_key=idempotency_key,
            events=[
                agent_service_pb2.EventLog(
                    message=e.message,
                    severity=self._log_level_to_severity(e.level),
                    timestamp=float_to_proto_timestamp(e.created_at),
                    metadata={k: v or '' for k, v in e.labels.items() if k},
                )
                for e in events
            ],
        )
        self.stub_agent.ReportEventLogs(request, timeout=self.timeout, metadata=self._request_metadata())

    def _log_level_to_severity(self, level: int) -> int:
        if level <= logging.DEBUG:
            return agent_service_pb2.EventLog.Severity.DEBUG
        if level <= logging.INFO:
            return agent_service_pb2.EventLog.Severity.INFO
        if level <= logging.WARN:
            return agent_service_pb2.EventLog.Severity.WARNING
        if level <= logging.ERROR:
            return agent_service_pb2.EventLog.Severity.ERROR
        return agent_service_pb2.EventLog.Severity.FATAL
