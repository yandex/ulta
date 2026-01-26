from google.api_core.exceptions import (
    ServerError,
    TooManyRequests,
)

LOADTESTING_UNAVAILABLE_ERRORS = (ServerError, TooManyRequests)


class CompositeException(Exception):
    def __init__(self, errors):
        d = {}
        for e in errors:
            key = (type(e), str(e))
            if key not in d:
                d[key] = e
        self.errors = list(d.values())

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
