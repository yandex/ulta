import grpc

from pathlib import Path
from typing import Union, Optional
from google.api_core.exceptions import from_grpc_error


def catch_exceptions(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except grpc.RpcError as error:
            raise from_grpc_error(error) from error

    return wrapper


def normalize_path(path: Union[Path, str, None]) -> Optional[str]:
    if not path:
        return path

    return Path(path).expanduser().absolute().as_posix()
