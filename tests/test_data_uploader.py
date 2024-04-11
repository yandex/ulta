import logging
import pytest
from multiprocessing import Queue
from unittest.mock import patch, call
from ulta.service.data_uploader import DataUploader


def test_data_uploader_default_workflow():
    with patch.object(DataUploader, 'send_data', return_value=0) as mock_send_data:
        q = Queue()
        uploader = DataUploader('job_id', q, logging.getLogger())
        uploader.start()
        assert uploader.thread.is_alive()
        q.put(0)
        q.put(1)
        q.put(None)
        uploader.thread.join(timeout=20)
        assert not uploader.thread.is_alive(), "Datauploader thread didn't finish"
        assert uploader.finished.is_set()
        uploader.finish()

        assert q.empty(), 'Datauploader should drain queue'
        mock_send_data.assert_has_calls([call(0), call(1)], any_order=False)


def test_data_uploader_finish_does_not_raise_if_not_started():
    uploader = DataUploader('job_id', Queue(), logging.getLogger())
    for _ in range(5):
        uploader.finish()


def test_data_uploader_raise_if_started_after_finish():
    uploader = DataUploader('job_id', Queue(), logging.getLogger())
    uploader.finish()
    with pytest.raises(RuntimeError):
        uploader.start()
