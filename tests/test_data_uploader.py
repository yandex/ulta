import logging
import threading
import time
import pytest
from multiprocessing import Queue
from unittest.mock import Mock
from ulta.common.interfaces import JobDataUploaderClient
from ulta.common.reporter import Reporter
from ulta.service.data_uploader import DataUploaderThread, TrailReportHandler, MonitoringReportHandler


def test_data_uploader_default_workflow():
    q_trail = Queue()
    q_mon = Queue()

    client = Mock(JobDataUploaderClient)
    client.prepare_test_data.side_effect = lambda data, stat: [data]
    client.prepare_monitoring_data.side_effect = lambda data: [data]

    def error_handler(e: Exception, logger: logging.Logger):
        logger.warning('Failed to send data: %s', e)

    trail_handler = TrailReportHandler(logging.getLogger(), client, 'job.id', error_handler, 10)
    mon_handler = MonitoringReportHandler(logging.getLogger(), client, 'job.id', error_handler, 10)
    uploader = DataUploaderThread(
        logging.getLogger(),
        Reporter(q_trail, logger=logging.getLogger(), handlers=trail_handler, use_exponential_backoff=True),
        Reporter(q_mon, logger=logging.getLogger(), handlers=mon_handler, use_exponential_backoff=True),
        [mon_handler, trail_handler],
        10,
    )

    q_trail.put((10, 11))
    q_trail.put((15, 17))
    q_mon.put(20)
    q_mon.put(21)
    uploader.start()
    q_trail.put(None)
    q_mon.put(None)
    time.sleep(1)
    uploader.stop()
    assert not uploader._thr.is_alive(), "Datauploader thread didn't finish"

    assert q_trail.empty(), 'Datauploader should drain queue'
    assert q_mon.empty(), 'Datauploader should drain queue'
    client.send_trails.assert_called_with('job.id', [10, 15])
    client.send_monitorings.assert_called_with('job.id', [20, 21])


def _send_data(q_trail: Queue, q_mon: Queue):
    time.sleep(1)
    q_trail.put((10, 11))
    q_trail.put((15, 17))
    q_mon.put(20)
    q_mon.put(21)
    time.sleep(1)
    q_trail.put(None)
    q_mon.put(None)


def test_data_uploader_shutdown_timeout():
    q_trail = Queue()
    q_mon = Queue()

    client = Mock(JobDataUploaderClient)
    client.prepare_test_data.side_effect = lambda data, stat: [data]
    client.prepare_monitoring_data.side_effect = lambda data: [data]

    def error_handler(e: Exception, logger: logging.Logger):
        logger.warning('Failed to send data: %s', e)

    trail_handler = TrailReportHandler(logging.getLogger(), client, 'job.id', error_handler, 10)
    mon_handler = MonitoringReportHandler(logging.getLogger(), client, 'job.id', error_handler, 10)
    uploader = DataUploaderThread(
        logging.getLogger(),
        Reporter(q_trail, logger=logging.getLogger(), handlers=trail_handler, use_exponential_backoff=True),
        Reporter(q_mon, logger=logging.getLogger(), handlers=mon_handler, use_exponential_backoff=True),
        [mon_handler, trail_handler],
        10,
    )

    uploader.start()
    threading.Thread(target=_send_data, args=(q_trail, q_mon)).start()
    uploader.stop()
    assert not uploader._thr.is_alive(), "Datauploader thread didn't finish"

    assert q_trail.empty(), 'Datauploader should drain queue'
    assert q_mon.empty(), 'Datauploader should drain queue'
    client.send_trails.assert_called_with('job.id', [10, 15])
    client.send_monitorings.assert_called_with('job.id', [20, 21])


def test_data_uploader_finish_does_not_raise_if_not_started():
    uploader = DataUploaderThread(logging.getLogger(), None, None, [], 10)
    for _ in range(5):
        uploader.stop()


def test_data_uploader_raise_if_started_after_stop():
    uploader = DataUploaderThread(logging.getLogger(), None, None, [], 10)
    uploader.stop()
    with pytest.raises(RuntimeError):
        uploader.start()
