import pytest
from io import StringIO
from unittest.mock import MagicMock
from ulta.common.job import Job
from ulta.common.exceptions import ArtifactUploadError
from ulta.service.log_uploader_service import LogUploaderService, LogReader, CHUNK_MAX_SIZE, MESSAGE_MAX_LENGTH


@pytest.mark.parametrize(
    ('data', 'expected', 'chunk_max_size', 'message_max_length'),
    [
        ('abc\nhahaha\nyo\n', [['abc\nhahaha\nyo\n']], None, 0),
        ('abc\nhahaha\nyo\n', [['abc\n', 'hahaha\nyo\n']], None, 10),
        ('abc\nhahaha\nyoyoyoy\n', [['abc\n'], ['hahaha\n'], ['yoyoyoy\n']], 1, 8),
        ('abc\nhahaha\nyoyoyoy\n', [['abc\n', 'hahaha\n'], ['yoyoyoy\n']], 2, 8),
        ('abc\n\n\n', [['abc\n\n\n']], None, 100),
        ('a' * 10, [['aaa', 'aaa', 'aaa', 'a']], None, 3),
        ('a' * 100 + 'abc\n' + 'k' * 99 + '\n', [['a' * 100, 'abc\n', 'k' * 99 + '\n']], None, 100),
        ('a' * 100 + 'abc\n' + 'k' * 99 + '\n', [['a' * 100, 'abc\n'], ['k' * 99 + '\n']], 2, 100),
    ],
)
def test_read_log_data(data, expected, chunk_max_size, message_max_length):
    chunk_max_size = chunk_max_size or CHUNK_MAX_SIZE
    message_max_length = message_max_length or MESSAGE_MAX_LENGTH
    data_stream = StringIO(data, newline='\n')
    reader = LogReader('', MagicMock())
    chunks = list(
        reader.read_log_data(data_stream, chunk_max_size=chunk_max_size, message_max_length=message_max_length)
    )
    assert chunks == expected


@pytest.mark.parametrize(
    'error',
    [
        Exception(),
        RuntimeError(),
    ],
)
def test_log_uploader_handles_errors(patch_log_uploader_send_logs, error):
    service = LogUploaderService(MagicMock(), MagicMock(), MagicMock())
    job = Job(
        id='123',
        config={'pandora': {'enabled': True}},
        tank_job_id='123',
        log_group_id='loggroup',
        artifact_dir_path='/tmp',
    )
    patch_log_uploader_send_logs.side_effect = error
    with pytest.raises(ArtifactUploadError):
        service.publish_artifacts(job)
    patch_log_uploader_send_logs.assert_called()
