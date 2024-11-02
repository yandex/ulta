import grpc
import logging
import typing

from google.protobuf.struct_pb2 import Struct
from google.protobuf.timestamp_pb2 import Timestamp
from yandex.cloud.logging.v1 import (
    log_ingestion_service_pb2,
    log_ingestion_service_pb2_grpc,
    log_entry_pb2,
    log_resource_pb2,
)
from ulta.common.utils import now, float_to_proto_timestamp
from ulta.common.interfaces import RemoteLoggingClient, LogMessage
from ulta.yc.ycloud import TokenProviderProtocol


class YCCloudLoggingClient(RemoteLoggingClient):
    def __init__(self, channel: grpc.Channel, token_provider: TokenProviderProtocol):
        self._token_provider = token_provider
        self.log_ingestion_service_stub = log_ingestion_service_pb2_grpc.LogIngestionServiceStub(channel)

    def _make_message(self, data: str | LogMessage, default_level: int, request_id: str | None):
        json_payload = Struct()
        if isinstance(data, LogMessage):
            timestamp = float_to_proto_timestamp(data.created_at)
            level = _map_log_level(data.level) if data.level else default_level
            message = data.message
            json_payload.update(data.labels)
        else:
            timestamp = Timestamp(seconds=int(now().timestamp()))
            level = default_level
            message = data
        if request_id is not None:
            json_payload.update({'request_id': request_id})
        return log_entry_pb2.IncomingLogEntry(
            message=message, level=level, json_payload=json_payload, timestamp=timestamp
        )

    def send_log(
        self,
        log_group_id: str,
        log_data: typing.Iterable[LogMessage | str],
        resource_type: str,
        resource_id: str,
        level: int | None = None,
        request_id: str | None = None,
        timeout: float = 5.0,
    ):
        destination = log_entry_pb2.Destination(log_group_id=log_group_id)
        log_resource = log_resource_pb2.LogEntryResource(type=resource_type, id=resource_id)
        default_level = _map_log_level(level) if level is not None else log_entry_pb2.LogLevel.Level.INFO
        entry_logs = [self._make_message(data, default_level, request_id) for data in log_data]

        request = log_ingestion_service_pb2.WriteRequest(
            destination=destination, resource=log_resource, entries=entry_logs
        )
        response = self.log_ingestion_service_stub.Write(
            request, timeout=timeout, metadata=self._request_metadata(request_id=request_id)
        )
        return response

    def _request_metadata(self, request_id):
        return [self._token_provider.get_auth_metadata(), ('x-request-id', request_id)]


def _map_log_level(level: int) -> int:
    if level < logging.DEBUG:
        return log_entry_pb2.LogLevel.Level.TRACE
    if level < logging.INFO:
        return log_entry_pb2.LogLevel.Level.DEBUG
    if level < logging.WARN:
        return log_entry_pb2.LogLevel.Level.INFO
    if level < logging.ERROR:
        return log_entry_pb2.LogLevel.Level.WARN
    if level < logging.FATAL:
        return log_entry_pb2.LogLevel.Level.ERROR
    return log_entry_pb2.LogLevel.Level.FATAL
