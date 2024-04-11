import pytest
from unittest.mock import patch, MagicMock
from ulta.yc.ycloud import (
    load_sa_key,
    JwtTokenRequester,
    AUDIENCE_URL_FROM_IAM_ENDPOINT,
    build_sa_key,
    SAKey,
    JWTError,
    AuthTokenProvider,
    _get_host_port_from_url,
    AuthError,
)

RAW_KEY_FILE_PATH = 'test_ycloud/raw_key.txt'
JSON_KEY_FILE_PATH = 'test_ycloud/json_key.json'


@pytest.fixture()
def patch_get_iam_token_from_metadata():
    with patch('ulta.yc.ycloud.get_iam_token_from_metadata') as p:
        p.return_value = ('some_token', 123124123)
        yield p


@pytest.mark.parametrize(
    'file_path, expected',
    [
        (RAW_KEY_FILE_PATH, SAKey('', '', 'raw_private_key_content')),
        (JSON_KEY_FILE_PATH, SAKey('json service account id', 'json key id', 'json_private_key')),
    ],
)
@pytest.mark.usefixtures('patch_cwd')
def test_load_sa_key(file_path, expected: SAKey):
    actual = load_sa_key(file_path)
    assert actual.sa_id == expected.sa_id
    assert actual.key_id == expected.key_id
    assert actual.key == expected.key


@pytest.mark.usefixtures('patch_get_iam_token_from_metadata')
@pytest.mark.parametrize(
    'args',
    [
        ({}),
        ({'unwanted': 'some_value'}),
    ],
)
def test_get_auth_token_requester_metadata(args, patch_get_iam_token_from_metadata):
    _, _ = AuthTokenProvider.get_auth_token_requester(**args, use_metadata_token=True)()
    patch_get_iam_token_from_metadata.assert_called_once()


@pytest.mark.parametrize('args, expected', [({'iam_token': 'some_token_payload'}, 'some_token_payload')])
def test_get_auth_token_requester_iam(args, expected):
    token, _ = AuthTokenProvider.get_auth_token_requester(**args)()
    assert token == expected


@pytest.mark.parametrize(
    'args',
    [
        ({}),
        ({'unwanted': 'some_value'}),
    ],
)
def test_get_auth_token_requester_no_provider(args):
    with pytest.raises(AuthError):
        _, _ = AuthTokenProvider.get_auth_token_requester(**args)()


@pytest.mark.parametrize(
    'args',
    [
        ({'sa_key': 'some_private_key', 'sa_id': 'some id'}),
        ({'sa_id': 'some id', 'sa_key_id': 'some key id'}),
        ({'sa_key_file': RAW_KEY_FILE_PATH}),
    ],
)
@pytest.mark.usefixtures('patch_cwd')
def test_get_auth_token_requester_raises_error(args):
    with pytest.raises(JWTError):
        _ = AuthTokenProvider.get_auth_token_requester(**args)


@pytest.mark.parametrize(
    'args, expected',
    [
        ({'sa_key': 'pk', 'sa_id': 'said', 'sa_key_id': 'sakeyid'}, SAKey('said', 'sakeyid', 'pk')),
        (
            {'sa_key_file': RAW_KEY_FILE_PATH, 'sa_id': 'said', 'sa_key_id': 'sakeyid'},
            SAKey('said', 'sakeyid', 'raw_private_key_content'),
        ),
        (
            {'sa_key_file': JSON_KEY_FILE_PATH, 'sa_id': 'said', 'sa_key_id': 'sakeyid'},
            SAKey('said', 'sakeyid', 'json_private_key'),
        ),
        ({'sa_key_file': JSON_KEY_FILE_PATH}, SAKey('json service account id', 'json key id', 'json_private_key')),
    ],
)
@pytest.mark.usefixtures('patch_cwd')
def test_build_sa_key(args, expected: SAKey):
    actual = build_sa_key(**args)

    assert actual is not None
    assert actual.sa_id == expected.sa_id
    assert actual.key_id == expected.key_id
    assert actual.key == expected.key


@pytest.mark.parametrize(
    'url, expected_host, expected_port',
    [
        ('localhost:443', 'localhost', 443),
        ('loadtesting.api.cloud.yandex.net:443', 'loadtesting.api.cloud.yandex.net', 443),
        ('https://api.cloud.yandex.net:1000', 'api.cloud.yandex.net', 1000),
        ('https://api.cloud.yandex.net/iam/v1/tokens', 'api.cloud.yandex.net', None),
        ('https://128.13.14.65:100', '128.13.14.65', 100),
        ('128.13.14.65:100', '128.13.14.65', 100),
    ],
)
def test_urlparser(url, expected_host, expected_port):
    host, port = _get_host_port_from_url(url)

    assert host == expected_host
    assert port == expected_port


@pytest.mark.parametrize(
    'url, expected',
    [
        ('localhost:443', 'https://localhost/iam/v1/tokens'),
        ('loadtesting.api.cloud.yandex.net:443', 'https://loadtesting.api.cloud.yandex.net/iam/v1/tokens'),
        ('https://api.cloud.yandex.net:1000', 'https://api.cloud.yandex.net/iam/v1/tokens'),
        ('https://api.cloud.yandex.net/iam/v1/tokens', 'https://api.cloud.yandex.net/iam/v1/tokens'),
        ('https://128.13.14.65:100', 'https://128.13.14.65/iam/v1/tokens'),
        ('128.13.14.65:100', 'https://128.13.14.65/iam/v1/tokens'),
        (None, 'https://iam.api.cloud.yandex.net/iam/v1/tokens'),
    ],
)
def test_audience_url(url, expected):
    sa_key = SAKey('id', 'key_id', 'asdfsd')
    assert (
        expected
        == JwtTokenRequester(url, sa_key, MagicMock(), audience_url=AUDIENCE_URL_FROM_IAM_ENDPOINT).audience_url
    )
