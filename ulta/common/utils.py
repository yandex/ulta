import functools
import grpc
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from google.api_core.exceptions import from_grpc_error
from tenacity import Retrying, wait_fixed, retry_if_exception, stop_after_attempt


def now():
    return datetime.now(timezone.utc)


def catch_exceptions(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except grpc.RpcError as error:
            raise from_grpc_error(error) from error

    return wrapper


def normalize_path(path: Path | str | None) -> Path | str:
    if not path:
        return path or ''

    return Path(path).expanduser().absolute().as_posix()


def ensure_dir(path: Path | str | None):
    path = normalize_path(path)
    if not path:
        raise FileNotFoundError(path)
    os.makedirs(path, exist_ok=True)
    with tempfile.NamedTemporaryFile('a', dir=path, delete=True):
        pass


RETRAYABLE_LT_CLIENT_CODES = {
    grpc.StatusCode.UNKNOWN,
    grpc.StatusCode.PERMISSION_DENIED,
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.UNAUTHENTICATED,
}


def retry_lt_client_call(func):
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
