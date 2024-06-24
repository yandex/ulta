from google.api_core.exceptions import (
    ServiceUnavailable,
    GatewayTimeout,
    TooManyRequests,
)

LOADTESTING_UNAVAILABLE_ERRORS = (ServiceUnavailable, GatewayTimeout, TooManyRequests)


class CompositeException(Exception):
    def __init__(self, errors):
        self.errors = errors

    def __str__(self) -> str:
        return 'Multiple errors occured:\n' + '\n'.join(str(e) for e in self.errors)


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
