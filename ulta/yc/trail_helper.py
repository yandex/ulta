from collections import defaultdict


def _value_to_float_safe(value):
    try:
        return float(value)
    except ValueError:
        return 0.0


def prepare_trail_data(proto_contract, data_item, stat_item):
    ts = data_item['ts']
    items = [
        _second_data_to_push_item(case_data, stat_item, ts, 0, case_name or '__NOTAG__')
        for case_name, case_data in data_item['tagged'].items()
    ]
    items.append(_second_data_to_push_item(data_item['overall'], stat_item, ts, 1, ''))

    trails = []
    for item in items:
        trail_data = item['trail']
        trail = proto_contract.Trail(
            overall=int(item['overall']),
            case_id=item['case'],
            time=trail_data['time'],
            reqps=int(trail_data['reqps']),
            resps=int(trail_data['resps']),
            expect=trail_data['expect'],
            input=int(trail_data['input']),
            output=int(trail_data['output']),
            connect_time=trail_data['connect_time'],
            send_time=trail_data['send_time'],
            latency=trail_data['latency'],
            receive_time=trail_data['receive_time'],
            threads=int(trail_data['threads']),
            q50=trail_data.get('q50'),
            q75=trail_data.get('q75'),
            q80=trail_data.get('q80'),
            q85=trail_data.get('q85'),
            q90=trail_data.get('q90'),
            q95=trail_data.get('q95'),
            q98=trail_data.get('q98'),
            q99=trail_data.get('q99'),
            q100=trail_data.get('q100'),
            http_codes=_build_codes(proto_contract, item['http_codes']),
            net_codes=_build_codes(proto_contract, item['net_codes']),
            time_intervals=_build_intervals(proto_contract, item['time_intervals']),
        )
        trails.append(trail)
    return trails


def prepare_monitoring_data(proto_contract, monitoring_data_item):
    metrics = defaultdict(list)
    for di in monitoring_data_item:
        ts = int(di['timestamp'])
        for host_name, data in di['data'].items():
            metrics[(host_name, ts, data['comment'])].extend(
                _json_metric_to_proto_message(proto_contract, data['metrics'])
            )

    return [
        proto_contract.MetricChunk(
            instance_host=key[0],
            timestamp=key[1],
            comment=key[2],
            data=chunk,
        )
        for key, chunk in metrics.items()
    ]


def _json_metric_to_proto_message(proto_contract, json_metrics):
    result = []
    for metric in json_metrics:
        # handle telegraf metric like 'custom:cpu-cpu0_idle_time', using 'cpu-cpu0' as metric type, 'idle_time' as metric_name
        parts = metric.split(':')[-1].split('_')
        result.append(
            proto_contract.Metric(
                metric_type=parts[0],
                metric_name='_'.join(parts[1:]),
                metric_value=_value_to_float_safe(json_metrics[metric]),
            )
        )
    return result


def _second_data_to_push_item(data, stat, timestamp, overall, case):
    api_data = {
        'overall': overall,
        'case': case,
        'net_codes': [],
        'http_codes': [],
        'time_intervals': [],
        'trail': {
            'time': str(timestamp),
            'reqps': stat['metrics']['reqps'],
            'resps': data['interval_real']['len'],
            'expect': data['interval_real']['total'] / 1000.0 / data['interval_real']['len'],
            'disper': 0,
            'self_load': 0,  # TODO abs(round(100 - float(data.selfload), 2)),
            'input': data['size_in']['total'],
            'output': data['size_out']['total'],
            'connect_time': data['connect_time']['total'] / 1000.0 / data['connect_time']['len'],
            'send_time': data['send_time']['total'] / 1000.0 / data['send_time']['len'],
            'latency': data['latency']['total'] / 1000.0 / data['latency']['len'],
            'receive_time': data['receive_time']['total'] / 1000.0 / data['receive_time']['len'],
            'threads': stat['metrics']['instances'],  # TODO
        },
    }

    for q, value in zip(data['interval_real']['q']['q'], data['interval_real']['q']['value']):
        api_data['trail']['q' + str(q)] = value / 1000.0

    for code, cnt in data['net_code']['count'].items():
        api_data['net_codes'].append({'code': int(code), 'count': int(cnt)})

    for code, cnt in data['proto_code']['count'].items():
        api_data['http_codes'].append({'code': int(code), 'count': int(cnt)})

    api_data['time_intervals'] = _convert_hist(data['interval_real']['hist'])
    return api_data


def _convert_hist(hist):
    data = hist['data']
    bins = hist['bins']
    return [
        {
            'from': 0,  # deprecated
            'to': b / 1000.0,
            'count': count,
        }
        for b, count in zip(bins, data)
    ]


def _build_codes(proto_contract, codes_data):
    codes = []
    for code in codes_data:
        codes.append(proto_contract.Trail.Codes(code=int(code['code']), count=int(code['count'])))
    return codes


def _build_intervals(proto_contract, intervals_data):
    intervals = []
    for interval in intervals_data:
        intervals.append(proto_contract.Trail.Intervals(to=interval['to'], count=int(interval['count'])))
    return intervals
