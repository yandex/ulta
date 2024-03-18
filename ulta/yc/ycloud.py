import grpc
import logging
import json
import jwt
import os
import requests
import time
from pathlib import Path
from requests.adapters import HTTPAdapter
from typing import Callable, Protocol
from urllib.parse import urlparse
from google.api_core.exceptions import Unauthenticated

from yandex.cloud.iam.v1.iam_token_service_pb2 import CreateIamTokenRequest
from yandex.cloud.iam.v1.iam_token_service_pb2_grpc import IamTokenServiceStub

# ====== HELPER ======
COMPUTE_METADATA_URL = 'http://169.254.169.254/computeMetadata/v1/?recursive=true'
COMPUTE_INSTANCE_SA_TOKEN_URL = 'http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token'
IAM_TOKEN_SERVICE_URL = "iam.api.cloud.yandex.net:443"
TOKEN_AUDIENCE_URL = 'https://iam.api.cloud.yandex.net/iam/v1/tokens'
AUDIENCE_URL_FROM_IAM_ENDPOINT = 'from_iam_endpoint'

TOKEN_TIMEOUT_SECONDS = 5 * 60
INFINITE_TIMEOUT_SECONDS = 5 * 365 * 24 * 60 * 60
METADATA_AGENT_VERSION_ATTR = 'agent-version'

LOGGER = logging.getLogger(__name__)


def get_metadata():
    url = COMPUTE_METADATA_URL
    try:
        session = requests.Session()
        session.mount(url, HTTPAdapter(max_retries=2))
        response = session.get(url, headers={"Metadata-Flavor": "Google"}).json()
        LOGGER.debug(f"Instance metadata {response}")
        return response
    except requests.exceptions.ConnectionError:
        LOGGER.warning('Compute metadata service is unavailable')
        return
    except Exception:
        LOGGER.exception("Couldn't get instance metadata of current vm")
        raise


def get_instance_metadata():
    response = get_metadata()
    if response:
        return response.get('instance')
    return


def get_instance_yandex_metadata():
    response = get_metadata()
    if response:
        return response.get('yandex')
    return


class AuthError(Unauthenticated):
    pass


class JWTError(AuthError):
    pass


class SAKey(object):
    def __init__(self, sa_id: str, key_id: str, key: str):
        self.sa_id = sa_id
        self.key_id = key_id
        self.key = key

    def validate(self) -> bool:
        missing_fields = [key for key, value in self.__dict__.items() if not value]
        if missing_fields:
            raise JWTError(f'All fields of SAKey must be set. Missing: {missing_fields}')
        return True


class TokenProviderProtocol(Protocol):
    def get_token(self) -> str:
        ...

    def get_auth_metadata(self) -> tuple[str, str]:
        ...


class AuthTokenProvider(TokenProviderProtocol):
    def __init__(self, iam_endpoint: str | None = None, **kwargs) -> None:
        """There are 3 auth methods supported. It will pick one method depending on provided args:
        * iam_token: auth using this token. NOTE: after token exires, this factory is no longer able to establish connection.
                     Use only for testing purposes

        * JWT auth: auth using JWT token. This method is enabled if one following cases satisfied:
                     1. 'sa_key' of SAKey type - will use this key to generate JWT tokens.
                     2. 'sa_key' of string type, 'sa_id', 'sa_key_id' - private key in PEM format, Service Account ID and Key ID
                     3. 'sa_key_file' - path to file with service account private key (json or pem file).
                            if json file provided - it will load SAKey from that file and use for auth
                            if pem file provided - it will require 'sa_id' and 'sa_key_id' parameters

                    see https://cloud.yandex.com/en-ru/docs/iam/operations/iam-token/create-for-sa#via-jwt for more details

        * compute metadata auth: will be used if no kwargs provided; viable only for instances running in Yandex Cloud Compute VMs

        :param iam_endpoint:
            URL of custom IAM
        :param **kwargs:
            See below

        :Keyword Arguments:
            * *iam_token* (``str``) --
                use Bearer token to authorize requests
            * *oauth_token* (``str``) --
                get oAuth token in https://oauth.yandex-team.ru for changing to IAM token
            * *sa_key* (``SAKey``) --
                Service Account key for JWT auth
            * *sa_key* (``str``) --
                Private key of Service Account key for JWT auth
            * *sa_key_file* (``str``) --
                Path to file containing Servie Account key for JWT auth
            * *sa_id* (``str``) --
                Service Account ID for JWT auth
            * *sa_key_id* (``str``) --
                Key ID for JWT auth"""

        self._iam_endpoint = iam_endpoint or IAM_TOKEN_SERVICE_URL
        self._token_requester = self.get_auth_token_requester(
            iam_endpoint=self._iam_endpoint, lazy_channel=self.get_iam_auth_channel, **kwargs
        )
        self._cached_iam_token = None
        self._expire_at = None

    def get_token(self):
        if not self._fresh():
            self._cached_iam_token, self._expire_at = self._token_requester()
            self._expire_at = min(self._expire_at, time.time() + TOKEN_TIMEOUT_SECONDS)

        return self._cached_iam_token

    def get_auth_metadata(self):
        return ("authorization", "Bearer " + self.get_token())

    def get_iam_auth_channel(self, options=None) -> grpc.Channel:
        return create_cloud_channel(self._iam_endpoint, channel_options=options)

    def _fresh(self):
        if self._cached_iam_token is None:
            return False
        return self._expire_at > time.time()

    @staticmethod
    def get_auth_token_requester(
        iam_endpoint: str | None = None,
        lazy_channel: Callable[[], grpc.Channel] | None = None,
        **kwargs,
    ) -> Callable[[], tuple[str, float]]:
        iam_token = kwargs.get('iam_token', '')
        if iam_token:
            LOGGER.info('get_auth_token_requester: using iam_token')

            def iam_token_requester():
                return iam_token, time.time() + INFINITE_TIMEOUT_SECONDS

            return iam_token_requester

        oauth_token = kwargs.get('oauth_token', '')
        if oauth_token:
            LOGGER.info('get_auth_token_requester: using oAuth_token')

            requester = OauthTokenRequester(
                iam_endpoint=iam_endpoint,
                oauth_token=oauth_token,
                channel=lazy_channel() if lazy_channel else None,
            )
            return requester.get_token

        sa_key = kwargs.get('sa_key', '')
        if not isinstance(sa_key, SAKey):
            sa_key = build_sa_key(**kwargs)

        if sa_key and sa_key.validate():
            LOGGER.info('get_auth_token_requester: using jwt auth')

            requester = JwtTokenRequester(
                iam_endpoint=iam_endpoint,
                sa_key=sa_key,
                channel=lazy_channel() if lazy_channel else None,
                audience_url=kwargs.get('audience_url'),
            )
            return requester.get_token

        if 'use_metadata_token' in kwargs and kwargs['use_metadata_token']:
            LOGGER.info('get_auth_token_requester: compute metadata auth')
            return get_iam_token_from_metadata

        raise AuthError(
            'Authentication provider not specified. Please provide Service Account key so agent could connect to Loadtesting'
        )


def get_iam_token_from_metadata() -> tuple[str, float]:
    url = COMPUTE_INSTANCE_SA_TOKEN_URL
    try:
        session = requests.Session()
        session.mount(url, HTTPAdapter(max_retries=2))
        token_request_time = time.time()
        raw_response = session.get(url, headers={"Metadata-Flavor": "Google"})
        response = raw_response.json()
        iam_token = response['access_token']
        expire_at = token_request_time + float(response.get('expires_in', TOKEN_TIMEOUT_SECONDS))
        LOGGER.debug("Get IAM token")
        return iam_token, expire_at
    except Exception as e:
        msg = "Couldn't get iam token for instance service account"
        LOGGER.exception(msg)
        raise AuthError(msg) from e


class JwtTokenRequester(object):
    def __init__(
        self, iam_endpoint: str | None, sa_key: SAKey, channel: grpc.Channel = None, audience_url: str | None = None
    ) -> None:
        self.iam_endpoint = iam_endpoint or IAM_TOKEN_SERVICE_URL

        audience_url = audience_url or TOKEN_AUDIENCE_URL
        if audience_url == AUDIENCE_URL_FROM_IAM_ENDPOINT:
            parsed_host, _ = _get_host_port_from_url(self.iam_endpoint)
            if not parsed_host:
                raise JWTError(
                    f'{self.iam_endpoint} is incorrect host for JWT audience. Use "iam.api.cloud.yandex.net:443"'
                )
            audience_url = f'https://{parsed_host}/iam/v1/tokens'

        self.audience_url = audience_url
        self.sa_key = sa_key
        if not sa_key or not self.sa_key.validate():
            raise JWTError('Service account key is mandatory for JWT auth')

        channel = channel or create_cloud_channel(self.iam_endpoint)
        self.iam_stub = IamTokenServiceStub(channel)

    def get_token(self) -> tuple[str, float]:
        """Get an IAM token by generating and sending a JWT token to the IAM service.

        Returns a tuple of (IAM token, expiry date) or raises an exception.
        """
        try:
            response = self.iam_stub.Create(CreateIamTokenRequest(jwt=self.create_jwt()))
            return response.iam_token, response.expires_at.ToSeconds()
        except Exception as e:
            raise JWTError("Couldn't get iam token from jwt.") from e

    def create_jwt(self) -> str:
        now = int(time.time())
        payload = {'aud': self.audience_url, 'iss': self.sa_key.sa_id, 'iat': now, 'exp': now + TOKEN_TIMEOUT_SECONDS}
        return jwt.encode(
            payload,
            self.sa_key.key,
            algorithm='PS256',
            headers={'kid': self.sa_key.key_id},
        )


class OauthTokenRequester(object):
    def __init__(self, iam_endpoint: str | None, oauth_token: str, channel: grpc.Channel = None) -> None:
        if not oauth_token:
            raise AuthError('Require not empty oAuth token.')

        self.iam_endpoint = iam_endpoint or IAM_TOKEN_SERVICE_URL
        self.oauth_token = oauth_token
        channel = channel or create_cloud_channel(self.iam_endpoint)
        self.iam_stub = IamTokenServiceStub(channel)

    def get_token(self) -> tuple[str, float]:
        """Exchange oAuth token to IAM token.

        Returns a tuple of (IAM token, expiry date) or raises an exception.
        """
        try:
            response = self.iam_stub.Create(CreateIamTokenRequest(yandex_passport_oauth_token=self.oauth_token))
            return response.iam_token, response.expires_at.ToSeconds()
        except Exception as e:
            raise AuthError("Couldn't get iam token from oAuth.") from e


def build_sa_key(**kwargs) -> SAKey | None:
    sa_private_key = kwargs.get('sa_key', '')
    sa_id = kwargs.get('sa_id', '')
    sa_key_id = kwargs.get('sa_key_id', '')
    sa_key_file = kwargs.get('sa_key_file', '')
    if sa_key_file:
        sa_key = load_sa_key(sa_key_file)
        sa_key.sa_id = sa_id or sa_key.sa_id
        sa_key.key_id = sa_key_id or sa_key.key_id
        return sa_key
    elif sa_id or sa_key_id or sa_private_key:
        return SAKey(sa_id, sa_key_id, sa_private_key)

    return None


def load_sa_key(file_path: str) -> SAKey:
    file_path = os.path.abspath(os.path.expanduser(file_path))
    try:
        return _load_sa_key_from_json(file_path)
    except json.JSONDecodeError as e:
        if e.pos > 1:
            raise JWTError('Failed to deserialize sa key file as json: incorrect json format')

    return _load_sa_key_from_pem(file_path)


def _load_sa_key_from_json(file_path: str) -> SAKey:
    with open(file_path, 'r') as f:
        key_data = json.load(f)

    return SAKey(key_data.get('service_account_id', ''), key_data.get('id', ''), key_data.get('private_key', ''))


def _load_sa_key_from_pem(file_path: str) -> SAKey:
    return SAKey(key_id='', sa_id='', key=Path(file_path).read_text())


def _get_host_port_from_url(url: str) -> tuple[str, str]:
    """Parse well-formed URLs and host:port pairs as well. Doesn't support ipv6 hosts."""
    result = urlparse(url)
    if not result.netloc and not url.startswith('//'):
        result = urlparse(f'//{url}')
    return result.hostname, result.port


def create_cloud_channel(backend_url, insecure_connection=False, channel_options=None) -> grpc.Channel:
    channel_options = channel_options or ()
    if insecure_connection:
        channel = grpc.insecure_channel(backend_url, channel_options + (('grpc.enable_http_proxy', 0),))
    else:
        channel = grpc.secure_channel(backend_url, grpc.ssl_channel_credentials(), channel_options)
    return channel
