import logging
import pytest
from unittest.mock import MagicMock
from ulta.service.tank_client import TankClient
from ulta.common.file_system import FS
from ulta.common.job import Job, JobPluginType
from ulta.common.job_status import AdditionalJobStatus


@pytest.mark.parametrize(
    ('response', 'exp_error', 'exp_error_type'),
    [
        ({}, '', None),
        ({'error': 'some error'}, 'some error', None),
        ({'error': 'some error', 'tank_msg': 'some tank_msg'}, 'some error', None),
        ({'tank_msg': 'some tank_msg'}, 'some tank_msg', 'internal'),
        ({'tank_msg': 'some tank_msg', 'exit_code': 1}, 'some tank_msg', 'internal'),
        ({'error': 'some error', 'exit_code': 1}, 'some error', None),
        ({'exit_code': 1}, 'Unknown error', None),
        ({'exit_code': 0}, '', None),
    ],
)
def test_extract_error(response, exp_error, exp_error_type):
    error, error_type = TankClient.extract_error(response)
    assert error == exp_error
    assert error_type == exp_error_type


@pytest.mark.parametrize(
    ('job_response', 'exp_status', 'exp_exit_code'),
    [
        ({}, AdditionalJobStatus.FAILED, 1),
        ({'status_code': 'FINISHED', 'exit_code': 21}, AdditionalJobStatus.AUTOSTOPPED, 21),
        ({'status_code': 'FINISHED', 'exit_code': 28}, AdditionalJobStatus.AUTOSTOPPED, 28),
        ({'status_code': 'FINISHED'}, 'FINISHED', 0),
        ({'status_code': 'TESTING'}, 'TESTING', None),
    ],
)
def test_parse_job_status(
    job_response,
    exp_status,
    exp_exit_code,
):
    status = TankClient.parse_job_status(job_response)
    assert status.status == exp_status
    assert status.exit_code == exp_exit_code


def test_finish_awaits_running_jobs(fs_mock: FS):
    client = TankClient(logging.getLogger(), fs_mock, MagicMock(), 'api_address')
    du, f1, f2 = MagicMock(), MagicMock(), MagicMock()
    client._finalizers = [f1, f2]
    client._data_uploader = du
    client.finish()
    du.stop.assert_called()
    f1.run.assert_called()
    f2.run.assert_called()


@pytest.mark.parametrize(
    ('config', 'expected_patch'),
    [
        (
            {'uploader': {'enabled': True, 'package': JobPluginType.UPLOADER, 'api_address': 'api_address'}},
            {'uploader': {'enabled': False}},
        ),
        (
            {'uploader': {'enabled': False, 'package': JobPluginType.UPLOADER, 'api_address': 'api_address'}},
            {},
        ),
        (
            {
                'first_uploader': {'enabled': True, 'package': JobPluginType.UPLOADER, 'api_address': 'api_address'},
                'overload_uploader': {
                    'enabled': True,
                    'package': JobPluginType.UPLOADER,
                    'api_address': 'other_address',
                },
                'some_other_uploader': {
                    'enabled': True,
                    'package': JobPluginType.UPLOADER,
                    'api_address': 'third_address',
                },
                'autostop': {'enabled': True, 'package': JobPluginType.AUTOSTOP, 'api_address': 'third_address'},
            },
            {'first_uploader': {'enabled': False}},
        ),
    ],
)
def test_disable_uploaders(config, expected_patch, fs_mock: FS):
    tank_client = TankClient(logging.getLogger(), fs_mock, MagicMock(), 'api_address')
    job = Job(id='id', config=config)
    patch = tank_client._generate_disable_data_uploaders_patch(job)
    assert expected_patch == patch
