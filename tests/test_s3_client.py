import os
from unittest import mock

from ulta.yc.s3_client import Boto3S3Client


def test_boto3_download_creates_parent_dir(tmp_path):
    # ключ с папкой в бакете: файл должен лечь в подкаталог, которого ещё нет
    fake_client = mock.Mock()

    def fake_download_file(bucket, key, path):
        with open(path, 'wb') as f:
            f.write(b'ammo')

    fake_client.download_file.side_effect = fake_download_file
    s3 = Boto3S3Client('http://s3', 'key', 'secret')
    storage_object = mock.Mock(object_storage_bucket='bucket', object_storage_filename='argus/data.ammo')
    target = tmp_path / 'test_data_1' / 'argus' / 'data.ammo'

    with mock.patch.object(s3, 'connect', return_value=fake_client):
        s3.download(storage_object, str(target))

    fake_client.download_file.assert_called_once_with('bucket', 'argus/data.ammo', str(target))
    assert os.path.isdir(target.parent)
    assert target.read_bytes() == b'ammo'
