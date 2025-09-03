#
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import random
import string
from unittest import mock

from airflow.api_fastapi.app import get_auth_manager
from airflow.composer.patches.rbac.utils import (
    _decode_inverting_proxy_jwt_with_public_keys,
    decode_inverting_proxy_jwt,
    get_or_register_user,
)
from airflow.providers.fab.auth_manager.models import User
from airflow.providers.fab.www.app import create_app

from tests_common.test_utils.config import conf_vars


class TestUtils:
    @mock.patch("airflow.composer.patches.rbac.utils.google.auth.default", autospec=True)
    @mock.patch("airflow.composer.patches.rbac.utils.AuthorizedSession", autospec=True)
    @mock.patch("airflow.composer.patches.rbac.utils.JWT_PUBLIC_KEYS_URL", "test-public-keys-url")
    @mock.patch("airflow.composer.patches.rbac.utils.INVERTING_PROXY_BACKEND_ID", "test-inverting-proxy-id")
    @mock.patch(
        "airflow.composer.patches.rbac.utils._decode_inverting_proxy_jwt_with_public_keys", autospec=True
    )
    def test_decode_inverting_proxy_jwt(
        self,
        decode_inverting_proxy_jwt_with_public_keys_mock,
        authorized_session_mock,
        google_auth_default_mock,
    ):
        credentials_mock = mock.Mock()
        google_auth_default_mock.return_value = credentials_mock, mock.Mock()
        request_mock = mock.Mock(
            return_value=mock.Mock(
                status_code=200,
                text="""-----BEGIN RSA PUBLIC KEY-----
MIIBCgKCAQEAo32rqcefi9tzT+kiGCluSM3raFR+xZYLo3DcEJPHh+DMx8koTp0N
rFs40FngwIwBiii0Vz/Pwnt/T2kpHAqbIniWSBHJnBcAEMmDSua4SV4Ho3JdBdy4
Erq0VfmMmCa93CdbHeh8Enyj47VP8pRoqz1INoShlnFQ+nOkGeHm7/EM9PPBHkAw
2KNWOk2QrVhO0OffhrgVA7bfrgLw1LnoB7DrE4eha0SJbxO2cNTDCvLtzNOIGJ7+
0dk/i+Bo0UOX6dDn87C/sOwOwOumR6KnznH8n0ClCy6ImRfdx5Bk6iBKgc98jG0L
DSGFOd9s7iFXBW5rlqywzGHrvT5i1beYKQIDAQAB
-----END RSA PUBLIC KEY-----
-----BEGIN RSA PUBLIC KEY-----
MIIBCgKCAQEAwRFGqHPkc8hBegJrSiM+c5/ZydAmOA/Uvn2bSvFxd7e5BrXd+KCi
rx0uB0qLtpafoOix+GWzS9baQY7TVouCDqbtjol8zRe34BYyBi7mg3ABHd5g8hcz
TkWqnS2BUTu8Zby4MLPj/dQUerumGhrc7/yxU0ykgc4skJM4o6LzNp7zL+80J9z8
Cw9VjyHc0o2cqenFk0JM1hGl9z2EpIu7XlQeCqO8qG79hKYCiLdF//jMvu0+h+A1
MjsvaR+VsF91s8nLKQz5dTubiriq6QXLQbqteQM5Q3BpFMgwZsAiUeB2O9A04ptB
0l8UDCnRwfkOOl+36xY14t8inOEhCYQMiQIDAQAB
-----END RSA PUBLIC KEY-----
""",
            )
        )
        authorized_session_mock.return_value = mock.Mock(request=request_mock)
        decode_inverting_proxy_jwt_with_public_keys_mock.return_value = "test-decoded-result"

        actual_result = decode_inverting_proxy_jwt("test-jwt")

        google_auth_default_mock.assert_called_once_with(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        authorized_session_mock.assert_called_once_with(credentials_mock)
        request_mock.assert_called_once_with(
            "GET",
            "test-public-keys-url",
            headers={
                "X-Inverting-Proxy-Backend-ID": "test-inverting-proxy-id",
            },
        )
        decode_inverting_proxy_jwt_with_public_keys_mock.assert_called_once_with(
            "test-jwt",
            [
                """-----BEGIN RSA PUBLIC KEY-----
MIIBCgKCAQEAo32rqcefi9tzT+kiGCluSM3raFR+xZYLo3DcEJPHh+DMx8koTp0N
rFs40FngwIwBiii0Vz/Pwnt/T2kpHAqbIniWSBHJnBcAEMmDSua4SV4Ho3JdBdy4
Erq0VfmMmCa93CdbHeh8Enyj47VP8pRoqz1INoShlnFQ+nOkGeHm7/EM9PPBHkAw
2KNWOk2QrVhO0OffhrgVA7bfrgLw1LnoB7DrE4eha0SJbxO2cNTDCvLtzNOIGJ7+
0dk/i+Bo0UOX6dDn87C/sOwOwOumR6KnznH8n0ClCy6ImRfdx5Bk6iBKgc98jG0L
DSGFOd9s7iFXBW5rlqywzGHrvT5i1beYKQIDAQAB
-----END RSA PUBLIC KEY-----\n""",
                """-----BEGIN RSA PUBLIC KEY-----
MIIBCgKCAQEAwRFGqHPkc8hBegJrSiM+c5/ZydAmOA/Uvn2bSvFxd7e5BrXd+KCi
rx0uB0qLtpafoOix+GWzS9baQY7TVouCDqbtjol8zRe34BYyBi7mg3ABHd5g8hcz
TkWqnS2BUTu8Zby4MLPj/dQUerumGhrc7/yxU0ykgc4skJM4o6LzNp7zL+80J9z8
Cw9VjyHc0o2cqenFk0JM1hGl9z2EpIu7XlQeCqO8qG79hKYCiLdF//jMvu0+h+A1
MjsvaR+VsF91s8nLKQz5dTubiriq6QXLQbqteQM5Q3BpFMgwZsAiUeB2O9A04ptB
0l8UDCnRwfkOOl+36xY14t8inOEhCYQMiQIDAQAB
-----END RSA PUBLIC KEY-----\n""",
            ],
        )
        assert actual_result == "test-decoded-result"

    @mock.patch("airflow.composer.patches.rbac.utils.google.auth.default", autospec=True)
    @mock.patch("airflow.composer.patches.rbac.utils.AuthorizedSession", autospec=True)
    def test_decode_inverting_proxy_jwt_public_keys_not_fetched(
        self,
        authorized_session_mock,
        google_auth_default_mock,
    ):
        google_auth_default_mock.return_value = mock.Mock(), mock.Mock()
        request_mock = mock.Mock(return_value=mock.Mock(status_code=500))
        authorized_session_mock.return_value = mock.Mock(request=request_mock)

        actual_result = decode_inverting_proxy_jwt("test-jwt")

        assert actual_result is None

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch("airflow.composer.patches.rbac.utils.RBAC_USER_REGISTRATION_ROLE", "Op")
    @mock.patch(
        "airflow.providers.fab.auth_manager.security_manager.override.FabAirflowSecurityManagerOverride.update_user_auth_stat",
        autospec=True,
    )
    def test_get_or_register_user(self, update_user_auth_stat_mock):
        create_app(enable_plugins=False)
        username = "".join(random.choice(string.ascii_uppercase) for _ in range(6))
        email = f"{username}@google.com"

        actual_user = get_or_register_user(username=username, email=email)

        update_user_auth_stat_mock.assert_called_once_with(get_auth_manager().appbuilder.sm, actual_user)

        assert isinstance(actual_user, User)
        assert actual_user.username == username
        assert actual_user.first_name == email
        assert actual_user.last_name == "-"
        assert actual_user.email == email
        assert len(actual_user.roles) == 1
        assert actual_user.roles[0].name == "Op"

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch("airflow.composer.patches.rbac.utils.RBAC_USER_REGISTRATION_ROLE", "Op")
    def test_get_or_register_user_already_registered(self):
        create_app(enable_plugins=False)
        username = "".join(random.choice(string.ascii_uppercase) for _ in range(6))
        email = f"{username}@google.com"

        get_or_register_user(username=username, email=email)
        # Try to register second time with same username and email.
        actual_user = get_or_register_user(username=username, email=email)

        assert actual_user.username == username

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch("airflow.composer.patches.rbac.utils.RBAC_USER_REGISTRATION_ROLE", "Op")
    def test_get_or_register_user_unsuccessful(self):
        create_app(enable_plugins=False)
        username = "".join(random.choice(string.ascii_uppercase) for _ in range(6))
        email = f"{username}@google.com"

        get_or_register_user(username=username, email=email)
        # Try to register second time with different username but same email. sm.add_user method should return
        # None in this case.
        actual_user = get_or_register_user(username=username + "2", email=email)

        assert actual_user is None

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch(
        "airflow.providers.fab.auth_manager.security_manager.override.FabAirflowSecurityManagerOverride.add_user",
        autospec=True,
    )
    @mock.patch(
        "airflow.providers.fab.auth_manager.security_manager.override.FabAirflowSecurityManagerOverride.update_user_auth_stat",
        autospec=True,
    )
    def test_get_or_register_user_retry_successful(self, update_user_auth_stat_mock, add_user_mock):
        create_app(enable_plugins=False)
        user_mock = mock.Mock()
        add_user_mock.side_effect = [False, user_mock]
        username = "".join(random.choice(string.ascii_uppercase) for _ in range(6))
        email = f"{username}@google.com"

        actual_user = get_or_register_user(username=username, email=email)

        assert actual_user == user_mock

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch(
        "airflow.providers.fab.auth_manager.security_manager.override.FabAirflowSecurityManagerOverride.add_user",
        autospec=True,
    )
    @mock.patch(
        "airflow.providers.fab.auth_manager.security_manager.override.FabAirflowSecurityManagerOverride.update_user_auth_stat",
        autospec=True,
    )
    def test_get_or_register_user_retry_unsuccessful(self, update_user_auth_stat_mock, add_user_mock):
        create_app(enable_plugins=False)
        add_user_mock.return_value = False
        username = "".join(random.choice(string.ascii_uppercase) for _ in range(6))
        email = f"{username}@google.com"

        actual_user = get_or_register_user(username=username, email=email)

        assert actual_user is None

    def test_decode_inverting_proxy_jwt_with_public_keys(self):
        inverting_proxy_jwt = (
            "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0LXVzZXJuYW1lIiwiZW1haWwiOiJ0ZXN0LWVtYWlsIn0"
            ".LrLq7-cR1ZULO4iDck0w4DvfVMq3la23AFhsXLzv261F5iee2cX0IHxGbt5eKqpIOQQuQkAj6YjOYJGVeA95nstN0XygfFy"
            "J_Oc1KXyJz5ExGpRx_pjLvBDcxBjr6V2mDr8ssCXeT9IKLDHJ6soEp7ReWg5BPkv_fhsoXnMfSVimjTSLZo_W8uiKtUlXaNv"
            "vvGYWYepBgAEBjz4BS_U4MoYirK1OL_vp6r0Qpsu0Bra6KTMkTm5sVtrN3gcB4XCksYUkElHjbCcrQBKnqptijhb64xHAvMC"
            "KCHaHXzVNqeOdlQK0lm72esWf1gMnKbeDopbe-SU8NRi2DKK3Q-4EKw"
        )
        public_keys = [
            "incorrect-public-key",
            """-----BEGIN PUBLIC KEY-----
MIIBITANBgkqhkiG9w0BAQEFAAOCAQ4AMIIBCQKCAQBFS07s5fq4x0xFooSb9spu
8PRBFhT1lTQo9+PBLznUVTdyPDO04eHMftgbCwAiCSWZ1COb9rTwFRkWL+TfXc2t
Upxk/l8Mb9jkJtBQ/JOFJ9jk3lZ6T0mCl7Kann+9dVC18JhIQNbke08dJWTdxxqX
8WcC++GGtBaQOrShWwQ6vnxItUeVSs/QFjKqr1KGemNhRdLqphcMoZ3UfoggYZ8p
sxusuBu42fUEP9F0rRpbV81xEmK1Ib5tdZ65mW+Dy9jjIh2nzojgXTKiXjB56vDk
N03Krbc3a4Rf9cxnGgo4gHEvY3bTb6ikqWQKJMaAtFJhz5gvXDzDHt1qso/okZE5
AgMBAAE=
-----END PUBLIC KEY-----
""",
        ]

        actual_result = _decode_inverting_proxy_jwt_with_public_keys(inverting_proxy_jwt, public_keys)

        assert actual_result == {
            "username": "test-username",
            "email": "test-email",
        }

    def test_decode_inverting_proxy_jwt_with_public_keys_incorrect_public_keys(self):
        actual_result = _decode_inverting_proxy_jwt_with_public_keys("test-jwt", ["incorrect-public-key"])

        assert actual_result is None
