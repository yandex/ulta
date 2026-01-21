import dataclasses
import functools
import grpc
import logging
import re
import typing
from datetime import datetime, timezone, timedelta
from pathlib import Path
from google.api_core.exceptions import from_grpc_error, GoogleAPICallError
from google.protobuf.timestamp_pb2 import Timestamp
from tenacity import Retrying, wait_fixed, retry_if_exception, stop_after_attempt


def now():
    return datetime.now(timezone.utc)


def catch_exceptions(func: typing.Callable):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except grpc.RpcError as error:
            raise from_grpc_error(error) from error

    return wrapper


@dataclasses.dataclass
class TrackRequestHeaders:
    client_request_id: str | None
    client_trace_id: str | None
    server_request_id: str | None
    server_trace_id: str | None

    @classmethod
    def from_grpc_metadata(cls, metadata: typing.Mapping[str, str]) -> 'TrackRequestHeaders':
        return cls(
            client_request_id=extract_metadata_value(metadata, 'x-client-request-id'),
            client_trace_id=extract_metadata_value(metadata, 'x-client-trace-id'),
            server_request_id=extract_metadata_value(metadata, 'x-request-id', 'x-server-request-id'),
            server_trace_id=extract_metadata_value(metadata, 'x-trace-id', 'x-server-request-id'),
        )

    def items(self) -> typing.Sequence[tuple[str, str | None]]:
        return (
            ('client-request-id', self.client_request_id),
            ('client-trace-id', self.client_trace_id),
            ('server-request-id', self.server_request_id),
            ('server-trace-id', self.server_trace_id),
        )


def extract_metadata_value(metadata: typing.Mapping[str, str], *names: str) -> str | None:
    for n in names:
        v = metadata.get(n.casefold(), None)
        if v is not None:
            return v

    return None


def exception_grpc_metadata(err: Exception) -> dict[str, str]:
    call = None
    if isinstance(err, GoogleAPICallError):
        if isinstance(err.response, grpc.Call):
            call = err.response
    elif isinstance(err, grpc.Call):
        call = err

    if call is None:
        return {}

    return dict(call.initial_metadata())


def normalize_path(path: Path | str | None) -> Path | str:
    if not path:
        return path or ''

    return Path(path).expanduser().absolute().as_posix()


RETRAYABLE_LT_CLIENT_CODES = {
    grpc.StatusCode.UNKNOWN,
    grpc.StatusCode.PERMISSION_DENIED,
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.UNAUTHENTICATED,
    grpc.StatusCode.ABORTED,
}


def retry_lt_client_call(func: typing.Callable):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        for attempt in Retrying(
            wait=wait_fixed(0.05),
            stop=stop_after_attempt(3),
            retry=retry_if_exception(lambda e: isinstance(e, grpc.RpcError) and e.code() in RETRAYABLE_LT_CLIENT_CODES),
            reraise=True,
        ):
            with attempt:
                return func(*args, **kwargs)

    return wrapper


def get_and_convert(
    value: typing.Any, cast: typing.Callable[[typing.Any], typing.Any], none_value: typing.Any = None
) -> typing.Any:
    if value is None:
        return none_value
    return cast(value)


def as_bool(value: typing.Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() not in ('', 'false', 'no')
    return bool(value)


def str_to_loglevel(value: typing.Any, default=logging.NOTSET) -> int:
    if value is None:
        return default
    str_value = str(value).strip().upper()

    if str_value == 'DISABLED':
        return logging.NOTSET
    if not str_value:
        return default
    result = logging.getLevelNamesMapping().get(str_value)
    if result is not None:
        return result

    try:
        int_value = int(str_value)
        if int_value in logging.getLevelNamesMapping().values():
            return int_value
    except ValueError:
        pass

    raise ValueError(f'Invalid log level {value}')


def str_to_timedelta(value: str | int) -> timedelta:
    if isinstance(value, int):
        return timedelta(seconds=value)
    if not isinstance(value, str):
        raise ValueError('str_to_timedelta value is expected to be str or int')
    suffixes = ['d', 'h', 'm', 's', 'ms', '(us|µs)']
    pattern = ''.join([f'(([0-9]+){suffix})?' for suffix in suffixes])
    pattern = f'(^{pattern}$)|(^[0-9]+$)'
    assert pattern == '(^(([0-9]+)d)?(([0-9]+)h)?(([0-9]+)m)?(([0-9]+)s)?(([0-9]+)ms)?(([0-9]+)(us|µs))?$)|(^[0-9]+$)'

    m = re.match(pattern, value.strip())
    if not m:
        raise ValueError(f'Invalid duration value: {value}; expected value in format 18h20m30s150ms')

    # (^[0-9]+$)
    if m.group(15):
        return timedelta(seconds=int(m.group(15)))

    return timedelta(
        days=get_and_convert(m.group(3), int, 0),
        hours=get_and_convert(m.group(5), int, 0),
        minutes=get_and_convert(m.group(7), int, 0),
        seconds=get_and_convert(m.group(9), int, 0),
        milliseconds=get_and_convert(m.group(11), int, 0),
        microseconds=get_and_convert(m.group(13), int, 0),
    )


def float_to_proto_timestamp(ts: float) -> Timestamp:
    seconds = int(ts)
    nanos = int((ts - seconds) * 1e9)
    return Timestamp(seconds=seconds, nanos=nanos)


def truncate_string(value: str, length: int | None, cut_in_middle=True) -> str:
    if length is None or not isinstance(value, str) or len(value) <= length:
        return value

    ph = '...'
    ph_len = len(ph)
    if length // 5 < ph_len:
        return value[:length]
    if cut_in_middle:
        # "long long long string" => "long l...tring"
        left_edge = length // 2
        right_edge = len(value) - left_edge + ph_len - (length % 2)
        return ''.join([value[:left_edge], ph, value[right_edge:]])
    else:
        return value[: length - ph_len] + ph
