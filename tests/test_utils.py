import pytest
from datetime import timedelta
from ulta.common.utils import str_to_timedelta, truncate_string


@pytest.mark.parametrize(
    'value, expected',
    [
        ('5d', timedelta(days=5)),
        ('4h', timedelta(hours=4)),
        ('3m', timedelta(minutes=3)),
        ('11s', timedelta(seconds=11)),
        ('7ms', timedelta(milliseconds=7)),
        ('16us', timedelta(microseconds=16)),
        ('17µs', timedelta(microseconds=17)),
        ('90', timedelta(seconds=90)),
        (90, timedelta(seconds=90)),
        ('0', timedelta(seconds=0)),
        (
            '1d15h24m57s600ms154us',
            timedelta(days=1, hours=15, minutes=24, seconds=57, milliseconds=600, microseconds=154),
        ),
        (
            '1d45h94m70s1600ms1054us',
            timedelta(days=1, hours=45, minutes=94, seconds=70, milliseconds=1600, microseconds=1054),
        ),
        ('3h30m0ms', timedelta(hours=3, minutes=30)),
        ('3d11s', timedelta(days=3, seconds=11)),
    ],
)
def test_str_to_timedelta(value, expected):
    assert str_to_timedelta(value) == expected


@pytest.mark.parametrize('value', ['123.55', '123f23s', '13:55:11'])
def test_str_to_timedelta_exception(value):
    with pytest.raises(ValueError):
        str_to_timedelta(value)


@pytest.mark.parametrize(
    'original, length, in_middle, expected',
    [
        ('some_string', 11, False, 'some_string'),
        ('', 10, False, ''),
        (None, 10, False, None),
        ('some_string', 9, False, 'some_stri'),
        ('very long long string', 15, False, 'very long lo...'),
        ('very long long string', None, False, 'very long long string'),
        ('some_string', 9, True, 'some_stri'),
        ('very long long string', 15, True, 'very lo...tring'),
    ],
)
def test_truncate_string(original, length, in_middle, expected):
    assert truncate_string(original, length, in_middle) == expected
