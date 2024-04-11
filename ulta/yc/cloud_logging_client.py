import grpc

from google.protobuf.struct_pb2 import Struct
from google.protobuf.timestamp_pb2 import Timestamp
from yandex.cloud.logging.v1 import (
    log_ingestion_service_pb2,
    log_ingestion_service_pb2_grpc,
    log_entry_pb2,
    log_resource_pb2,
)
from ulta.common.interfaces import CloudLoggingClient
from ulta.common.utils import now
from ulta.yc.ycloud import TokenProviderProtocol


class YCCloudLoggingClient(CloudLoggingClient):
    def __init__(self, channel: grpc.Channel, token_provider: TokenProviderProtocol):
        self._token_provider = token_provider
        self.log_ingestion_service_stub = log_ingestion_service_pb2_grpc.LogIngestionServiceStub(channel)

    def send_log(self, log_group_id, log_data, resource_type, resource_id, level=None, request_id=None, timeout=5.0):
        destination = log_entry_pb2.Destination(log_group_id=log_group_id)
        log_resource = log_resource_pb2.LogEntryResource(type=resource_type, id=resource_id)
        level = level or log_entry_pb2.LogLevel.Level.INFO
        json_payload = Struct()
        json_payload.update({'request_id': request_id})
        current_time = Timestamp(seconds=int(now().timestamp()))
        entry_logs = [
            log_entry_pb2.IncomingLogEntry(message=data, level=level, json_payload=json_payload, timestamp=current_time)
            for data in log_data
        ]

        request = log_ingestion_service_pb2.WriteRequest(
            destination=destination, resource=log_resource, entries=entry_logs
        )
        response = self.log_ingestion_service_stub.Write(
            request, timeout=timeout, metadata=self._request_metadata(request_id=request_id)
        )
        return response

    def _request_metadata(self, request_id):
        return [self._token_provider.get_auth_metadata(), ('x-request-id', request_id)]
