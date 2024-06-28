import boto3
import boto3.session
import os
import requests
import shutil
import threading
from cachetools import TTLCache
from requests.adapters import HTTPAdapter
from ulta.common.exceptions import ObjectStorageError
from ulta.common.interfaces import S3Client
from ulta.yc.ycloud import TokenProviderProtocol


class YCS3Client(S3Client):
    def __init__(self, object_storage_url: str, token_provider: TokenProviderProtocol):
        self.object_storage_url = object_storage_url
        self.token_provider = token_provider

    def download(self, storage_object, path_to_download):
        s3_bucket, s3_filename = storage_object.object_storage_bucket, storage_object.object_storage_filename
        try:
            url = f'{self.object_storage_url}/{s3_bucket}/{s3_filename}'
            session = requests.Session()
            session.mount(url, HTTPAdapter(max_retries=3))
            with session.get(
                url,
                headers={'X-YaCloud-SubjectToken': self.token_provider.get_token(), 'Accept-Encoding': 'identity'},
                stream=True,
            ) as r:
                r.raise_for_status()
                os.makedirs(os.path.dirname(path_to_download), exist_ok=True)
                with open(path_to_download, 'wb') as file:
                    shutil.copyfileobj(r.raw, file)
        except requests.RequestException as error:
            raise ObjectStorageError(
                f"Couldn't download file {s3_filename} from bucket {s3_bucket}: {error}"
            ) from error

    def upload(self, source_file, s3_filename, s3_bucket):
        with open(source_file, 'rb') as f:
            try:
                with requests.put(
                    f'{self.object_storage_url}/{s3_bucket}/{s3_filename}',
                    headers={'X-YaCloud-SubjectToken': self.token_provider.get_token(), 'Accept-Encoding': 'identity'},
                    data=f,
                    stream=True,
                ) as r:
                    r.raise_for_status()
            except requests.RequestException as e:
                raise ObjectStorageError(f"Couldn't upload file {source_file} to {s3_bucket}: {str(e)}") from e


class Boto3S3Client(S3Client):
    def __init__(
        self, endpoint_url, access_key_id: str, secret_key: str, max_connection_cache_size=5, max_connection_ttl=3600
    ) -> None:
        self._endpoint_url = endpoint_url
        self._access_key_id = access_key_id
        self._secret_key = secret_key
        self._cache = TTLCache(maxsize=max_connection_cache_size, ttl=max_connection_ttl)

    def connect(self, thread_id):
        client = self._cache.get(thread_id)
        if client is None:
            session = boto3.session.Session()
            client = session.client(
                's3',
                endpoint_url=self._endpoint_url,
                aws_access_key_id=self._access_key_id,
                aws_secret_access_key=self._secret_key,
            )
            self._cache[thread_id] = client
        return client

    def download(self, storage_object, path_to_download):
        client = self.connect(threading.get_ident())
        client.download_file(
            storage_object.object_storage_bucket, storage_object.object_storage_filename, path_to_download
        )

    def upload(self, source_file, s3_filename, s3_bucket):
        client = self.connect(threading.get_ident())
        client.upload_file(source_file, s3_bucket, s3_filename)
