from ulta.common.config import ExternalConfigLoader, UltaConfig
from ulta.common.utils import get_and_convert
from ulta.yc.ycloud import get_instance_metadata, get_instance_yandex_metadata, METADATA_AGENT_VERSION_ATTR

YANDEX_COMPUTE = 'YANDEX_CLOUD_COMPUTE'


class YandexCloudConfigLoader(ExternalConfigLoader):
    def name(self):
        return 'compute_metadata'

    def __call__(self, config: UltaConfig):
        METADATA_HOST_ATTR = 'server-host'
        METADATA_PORT_ATTR = 'server-port'
        METADATA_REQUEST_INTERVAL = 'request-frequency'  # seconds
        METADATA_REPORTER_INTERVAL = 'reporter-interval'  # seconds
        METADATA_LOGGING_HOST_ATTR = 'cloud-helper-logging-host'
        METADATA_LOGGING_PORT_ATTR = 'cloud-helper-logging-port'
        METADATA_OBJECT_STORAGE_URL_ATTR = 'cloud-helper-object-storage-url'
        METADATA_LT_CREATED_ATTR = 'loadtesting-created'
        METADATA_AGENT_NAME_ATTR = 'agent-name'
        METADATA_FOLDER_ID_ATTR = 'folder-id'
        YANDEX_METADATA_FOLDER_ID_ATTR = 'folderId'

        metadata: dict = get_instance_metadata() or {}
        yandex_metadata: dict = get_instance_yandex_metadata() or {}
        attrs: dict = metadata.get('attributes', {})

        config.backend_service_url = build_backend_url(attrs.get(METADATA_HOST_ATTR), attrs.get(METADATA_PORT_ATTR))
        config.logging_service_url = build_backend_url(
            attrs.get(METADATA_LOGGING_HOST_ATTR), attrs.get(METADATA_LOGGING_PORT_ATTR)
        )
        config.object_storage_url = attrs.get(METADATA_OBJECT_STORAGE_URL_ATTR)
        config.request_interval = get_and_convert(attrs.get(METADATA_REQUEST_INTERVAL), int)
        config.reporter_interval = get_and_convert(attrs.get(METADATA_REPORTER_INTERVAL), int)
        config.compute_instance_id = metadata.get('id')
        config.agent_version = attrs.get(METADATA_AGENT_VERSION_ATTR)
        config.instance_lt_created = get_and_convert(attrs.get(METADATA_LT_CREATED_ATTR), bool)
        config.agent_name = attrs.get(METADATA_AGENT_NAME_ATTR)
        config.folder_id = attrs.get(METADATA_FOLDER_ID_ATTR, yandex_metadata.get(YANDEX_METADATA_FOLDER_ID_ATTR))

    @classmethod
    def should_apply(cls, environment: str) -> bool:
        return environment == YANDEX_COMPUTE

    @classmethod
    def env_type(cls) -> str:
        return YANDEX_COMPUTE


def build_backend_url(host, port):
    if host is None:
        return None

    if ':' in host and not host.startswith('['):
        target = '[{host}]'
    else:
        target = host
    if port:
        target = target + ':' + str(port)
    return target
