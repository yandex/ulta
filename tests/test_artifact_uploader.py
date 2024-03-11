import pytest
import os
from unittest.mock import MagicMock
from pathlib import Path
from ulta.common.cancellation import Cancellation, CancellationRequest
from ulta.common.exceptions import ArtifactUploadError
from ulta.common.job import Job, ArtifactSettings
from ulta.service.artifact_uploader import _relative_to, ROOT_SEGMENT, S3ArtifactUploader


TEST_FOLDER_PATH = 'test_artifact/test_folder/'
OTHER_FOLDER_PATH = 'test_artifact/other_folder/'
ALL_TEST_FOLDER_PATHS = {
    'root.zip',
    'phout.log',
    'tank.log',
    'test.yaml',
    'ammo_folder/a1.ammo',
    'ammo_folder/a2.ammo',
    'ammo_folder/jmeter.zip',
    'ammo_folder/sub/sub/another.ammo',
    'ammo_folder/sub/sub/scenario.txt',
}


def normalize_for_test(path, start) -> str:
    return os.path.relpath(os.path.abspath(path), os.path.abspath(start))


@pytest.mark.parametrize(
    'root, path, expected',
    [
        ('/haha/seg2', '/haha/seg2/yes', 'yes'),
        ('/haha/seg2', '/haha/seg2/yes/yes', 'yes/yes'),
        ('/haha/seg2', '/other/haha/seg2/yes/yes', ROOT_SEGMENT + '/other/haha/seg2/yes/yes'),
        ('haha/seg2', 'haha/seg2/yes/yes', 'yes/yes'),
    ],
)
def test__relative_to(root, path, expected):
    assert expected == _relative_to(path, root)
    assert expected == _relative_to(path, Path(root))
    assert expected == _relative_to(Path(path), root)
    assert expected == _relative_to(
        Path(path),
        Path(root),
    )


@pytest.mark.parametrize(
    'dir_path, filter_include, filter_exclude, expected',
    [
        (TEST_FOLDER_PATH, [], [], []),
        (TEST_FOLDER_PATH, ['*'], [], ALL_TEST_FOLDER_PATHS),
        (
            TEST_FOLDER_PATH,
            ['*.ammo', 'tank.log'],
            [],
            ['tank.log', 'ammo_folder/a1.ammo', 'ammo_folder/a2.ammo', 'ammo_folder/sub/sub/another.ammo'],
        ),
        (
            TEST_FOLDER_PATH,
            ['**/*.ammo', 'tank.log'],
            ['**/another.ammo'],
            ['tank.log', 'ammo_folder/a1.ammo', 'ammo_folder/a2.ammo'],
        ),
        (
            TEST_FOLDER_PATH,
            ['**/*.ammo', 'tank.log'],
            ['**/sub/*'],
            ['tank.log', 'ammo_folder/a1.ammo', 'ammo_folder/a2.ammo'],
        ),
        (TEST_FOLDER_PATH, ['*'], ['*.zip'], ALL_TEST_FOLDER_PATHS - {'root.zip', 'ammo_folder/jmeter.zip'}),
    ],
)
@pytest.mark.usefixtures('patch_cwd')
def test_collect_files(dir_path, filter_include, filter_exclude, expected):
    uploader = S3ArtifactUploader(MagicMock(), MagicMock(), Cancellation(), MagicMock())
    actual = uploader._collect_files(Path(dir_path), filter_include, filter_exclude)
    actual = [normalize_for_test(p, dir_path) for p in actual]
    assert not (set(actual) - set(expected)), 'Actual contains excess items: %s' % (set(actual) - set(expected))
    assert not (set(expected) - set(actual)), 'Actual is missing items: %s' % (set(expected) - set(actual))


@pytest.mark.parametrize(
    'error, expected_error',
    [
        (Exception(), ArtifactUploadError),
        (RuntimeError(), ArtifactUploadError),
        (CancellationRequest(), CancellationRequest),
        (ArtifactUploadError(), ArtifactUploadError),
    ],
)
def test_artifact_uploader_handles_errors(
    patch_s3_uploader_collect_artifacts, patch_s3_uploader_upload_artifacts, error, expected_error
):
    uploader = S3ArtifactUploader(MagicMock(), MagicMock(), Cancellation(), MagicMock())
    job = Job(
        id='123',
        config={'pandora': {'enabled': True}},
        tank_job_id='123',
        upload_artifact_settings=ArtifactSettings(
            output_bucket='bucket', output_name='name', is_archive=False, filter_include=['*'], filter_exclude=[]
        ),
        artifact_dir_path='/tmp',
    )
    patch_s3_uploader_collect_artifacts.return_value = [('name', 'path')]
    patch_s3_uploader_upload_artifacts.side_effect = error
    with pytest.raises(expected_error):
        uploader.publish_artifacts(job)
    patch_s3_uploader_upload_artifacts.assert_called()
