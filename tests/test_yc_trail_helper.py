import ulta.yc.trail_helper as trail_helper
from yandex.cloud.loadtesting.agent.v1 import monitoring_service_pb2, trail_service_pb2


SAMPLE_MONITORING_METRICS = {
    'Memory_buff': 1026465792,
    'custom:mem_commit_limit': 33079422976,
    'custom:cpu-cpu-total_usage_idle': 96.79043423670709,
    'System_la5': 1.12,
    'custom:cpu-cpu4_usage_system': 0.9999999999308784,
    'custom:mem_high_free': 0,
    'custom:network_message': 'connection ok',
}
SAMPLE_MONITORING_DATA = [
    {
        'timestamp': 1663666609,
        'data': {
            'localhost': {
                'comment': 'some comment',
                'metrics': {
                    'System_la5': 1.12,
                    'custom:network_message': 'connection ok',
                },
            }
        },
    },
    {
        'timestamp': 1663666609,
        'data': {
            'localhost': {
                'comment': 'some comment',
                'metrics': {
                    'custom:mem_commit_limit': 33079422976,
                },
            }
        },
    },
]


def test_json_metric_to_proto_message():
    expected = [
        monitoring_service_pb2.Metric(
            metric_type='Memory',
            metric_name='buff',
            metric_value=1026465792,
        ),
        monitoring_service_pb2.Metric(
            metric_type='mem',
            metric_name='commit_limit',
            metric_value=33079422976,
        ),
        monitoring_service_pb2.Metric(
            metric_type='cpu-cpu-total',
            metric_name='usage_idle',
            metric_value=96.79043423670709,
        ),
        monitoring_service_pb2.Metric(
            metric_type='System',
            metric_name='la5',
            metric_value=1.12,
        ),
        monitoring_service_pb2.Metric(
            metric_type='cpu-cpu4',
            metric_name='usage_system',
            metric_value=0.9999999999308784,
        ),
        monitoring_service_pb2.Metric(
            metric_type='mem',
            metric_name='high_free',
            metric_value=0,
        ),
        monitoring_service_pb2.Metric(
            metric_type='network',
            metric_name='message',
            metric_value=0,
        ),
    ]

    assert expected == trail_helper._json_metric_to_proto_message(monitoring_service_pb2, SAMPLE_MONITORING_METRICS)


def test_json_monitoring_data_item_to_proto_request_messages():
    expected = [
        monitoring_service_pb2.MetricChunk(
            timestamp=1663666609,
            comment='some comment',
            instance_host='localhost',
            data=[
                monitoring_service_pb2.Metric(
                    metric_type='System',
                    metric_name='la5',
                    metric_value=1.12,
                ),
                monitoring_service_pb2.Metric(
                    metric_type='network',
                    metric_name='message',
                    metric_value=0,
                ),
                monitoring_service_pb2.Metric(
                    metric_type='mem',
                    metric_name='commit_limit',
                    metric_value=33079422976,
                ),
            ],
        )
    ]

    assert expected == trail_helper.prepare_monitoring_data(monitoring_service_pb2, SAMPLE_MONITORING_DATA)


def test_json_trail_data_item_to_proto_request_messages():
    data_item = {
        'tagged': {
            'Technology': {
                'size_in': {'max': 315, 'total': 315, 'len': 1, 'min': 315},
                'latency': {'max': 521, 'total': 521, 'len': 1, 'min': 521},
                'interval_real': {
                    'q': {
                        'q': [50, 75, 80, 85, 90, 95, 98, 99, 100],
                        'value': [797.0, 797.0, 797.0, 797.0, 797.0, 797.0, 797.0, 797.0, 797.0],
                    },
                    'min': 797,
                    'max': 797,
                    'len': 1,
                    'hist': {'data': [1], 'bins': [800.0]},
                    'total': 797,
                },
                'interval_event': {'max': 670, 'total': 670, 'len': 1, 'min': 670},
                'receive_time': {'max': 56, 'total': 56, 'len': 1, 'min': 56},
                'connect_time': {'max': 208, 'total': 208, 'len': 1, 'min': 208},
                'proto_code': {'count': {'404': 1}},
                'size_out': {'max': 31, 'total': 31, 'len': 1, 'min': 31},
                'send_time': {'max': 12, 'total': 12, 'len': 1, 'min': 12},
                'net_code': {'count': {'0': 1}},
            }
        },
        'overall': {
            'size_in': {'max': 315, 'total': 315, 'len': 1, 'min': 315},
            'latency': {'max': 521, 'total': 521, 'len': 1, 'min': 521},
            'interval_real': {
                'q': {
                    'q': [50, 75, 80, 85, 90, 95, 98, 99, 100],
                    'value': [797.0, 797.0, 797.0, 797.0, 797.0, 797.0, 797.0, 797.0, 797.0],
                },
                'min': 797,
                'max': 797,
                'len': 1,
                'hist': {'data': [1], 'bins': [800.0]},
                'total': 797,
            },
            'interval_event': {'max': 670, 'total': 670, 'len': 1, 'min': 670},
            'receive_time': {'max': 56, 'total': 56, 'len': 1, 'min': 56},
            'connect_time': {'max': 208, 'total': 208, 'len': 1, 'min': 208},
            'proto_code': {'count': {'404': 1}},
            'size_out': {'max': 31, 'total': 31, 'len': 1, 'min': 31},
            'send_time': {'max': 12, 'total': 12, 'len': 1, 'min': 12},
            'net_code': {'count': {'0': 1}},
        },
        'ts': 1502376593,
    }

    stat_item = {
        'metrics': {
            'reqps': 200,
            'instances': 51,
        }
    }
    expected = [
        trail_service_pb2.Trail(
            case_id='Technology',
            time='1502376593',
            reqps=200,
            resps=1,
            expect=0.797,
            input=315,
            output=31,
            connect_time=0.208,
            send_time=0.012,
            latency=0.521,
            receive_time=0.056,
            threads=51,
            q50=0.797,
            q75=0.797,
            q80=0.797,
            q85=0.797,
            q90=0.797,
            q95=0.797,
            q98=0.797,
            q99=0.797,
            q100=0.797,
            http_codes=[
                trail_service_pb2.Trail.Codes(
                    code=404,
                    count=1,
                )
            ],
            net_codes=[
                trail_service_pb2.Trail.Codes(
                    code=0,
                    count=1,
                )
            ],
            time_intervals=[
                trail_service_pb2.Trail.Intervals(
                    to=0.8,
                    count=1,
                )
            ],
        ),
        trail_service_pb2.Trail(
            overall=1,
            time='1502376593',
            reqps=200,
            resps=1,
            expect=0.797,
            input=315,
            output=31,
            connect_time=0.208,
            send_time=0.012,
            latency=0.521,
            receive_time=0.056,
            threads=51,
            q50=0.797,
            q75=0.797,
            q80=0.797,
            q85=0.797,
            q90=0.797,
            q95=0.797,
            q98=0.797,
            q99=0.797,
            q100=0.797,
            http_codes=[
                trail_service_pb2.Trail.Codes(
                    code=404,
                    count=1,
                )
            ],
            net_codes=[
                trail_service_pb2.Trail.Codes(
                    code=0,
                    count=1,
                )
            ],
            time_intervals=[
                trail_service_pb2.Trail.Intervals(
                    to=0.8,
                    count=1,
                )
            ],
        ),
    ]

    requests = trail_helper.prepare_trail_data(trail_service_pb2, data_item, stat_item)
    assert expected == requests
