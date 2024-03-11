import logging
from pytest import raises
from multiprocessing import Queue
from unittest.mock import MagicMock, patch, call
from ulta.service.data_uploader import DataUploader

_TEST_LOGGER = logging.getLogger(__name__)


def test_data_uploader_default_workflow():
    with patch.object(DataUploader, 'send_data', return_value=0) as mock_send_data:
        q = Queue()
        uploader = DataUploader('job_id', q, _TEST_LOGGER)
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
    uploader = DataUploader('job_id', MagicMock(), _TEST_LOGGER)
    for _ in range(5):
        uploader.finish()


def test_data_uploader_raise_if_started_after_finish():
    uploader = DataUploader('job_id', MagicMock(), _TEST_LOGGER)
    uploader.finish()
    with raises(RuntimeError):
        uploader.start()
