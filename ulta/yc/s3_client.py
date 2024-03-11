import os
import requests
import shutil
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
