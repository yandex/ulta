from google.api_core.exceptions import (
    ServiceUnavailable,
    GatewayTimeout,
    TooManyRequests,
)

LOADTESTING_UNAVAILABLE_ERRORS = (ServiceUnavailable, GatewayTimeout, TooManyRequests)


class ArtifactUploadError(Exception):
    pass


class ObjectStorageError(Exception):
    pass


class TankError(Exception):
    pass


class InvalidJobDataError(Exception):
    pass


class JobStoppedError(Exception):
    pass


class JobNotExecutedError(Exception):
    pass
