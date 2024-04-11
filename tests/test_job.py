import pytest
from ulta.common.job import Job, Generator, JobPluginType


@pytest.mark.parametrize(
    ('config', 'exp_generator'),
    [
        ({'pandora': {'enabled': True, 'package': 'yandextank.plugins.Pandora'}}, Generator.PANDORA),
        ({'phantom': {'enabled': True, 'package': 'yandextank.plugins.Phantom'}}, Generator.PHANTOM),
        ({'not_jmeter': {'enabled': True, 'package': 'yandextank.plugins.JMeter'}}, Generator.JMETER),
        ({'phantom': {'enabled': False}}, Generator.UNKNOWN),
    ],
)
def test_generator(config, exp_generator):
    assert Job(id='id', config=config).generator == exp_generator


@pytest.mark.parametrize(
    ('config', 'exp_enabled'),
    [
        ({'autostop': {'enabled': True, 'package': JobPluginType.AUTOSTOP}}, True),
        (
            {
                'autostop': {'enabled': False, 'package': JobPluginType.AUTOSTOP},
                'autostop2': {'enabled': True, 'package': JobPluginType.AUTOSTOP},
            },
            True,
        ),
        ({'autostop': {'enabled': False, 'package': JobPluginType.AUTOSTOP}}, False),
        ({'autostop': {'enabled': True, 'package': JobPluginType.TELEGRAF}}, False),
    ],
)
def test_job_plugin_enabled(config, exp_enabled):
    assert Job(id='id', config=config).plugin_enabled(JobPluginType.AUTOSTOP) == exp_enabled


@pytest.mark.parametrize(
    ('config', 'expected_plugins'),
    [
        ({'uploader': {'enabled': True, 'package': JobPluginType.UPLOADER}}, {'uploader'}),
        (
            {
                'uploader': {'enabled': False, 'package': JobPluginType.UPLOADER},
                'overload_uploader': {'enabled': True, 'package': JobPluginType.UPLOADER},
                'some_other_uploader': {'enabled': True, 'package': JobPluginType.UPLOADER},
            },
            {'overload_uploader', 'some_other_uploader'},
        ),
        (
            {
                'uploader': {'enabled': False, 'package': JobPluginType.UPLOADER},
                'autostop': {'enabled': True, 'package': JobPluginType.AUTOSTOP},
            },
            set(),
        ),
        ({'uploader': {'enabled': True, 'package': JobPluginType.AUTOSTOP}}, set()),
    ],
)
def test_job_get_plugins(config, expected_plugins):
    plugins = Job(id='id', config=config).get_plugins(JobPluginType.UPLOADER)
    assert set([k for k, v in plugins]) == expected_plugins
