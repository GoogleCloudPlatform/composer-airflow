#
# Copyright 2020 Google LLC
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
import os
import random
import shutil
import string
import unittest
from unittest import mock

import jwt
from google.auth.transport import requests

from airflow.configuration import WEBSERVER_CONFIG
from airflow.security import permissions
from airflow.composer.security_manager import _get_first_and_last_name
from airflow.www import app
from tests.test_utils.api_connexion_utils import create_user
from tests.test_utils.config import conf_vars


class TestBase(unittest.TestCase):
    CURRENT_DIRECTORY = os.path.dirname(os.path.realpath(__file__))
    WEBSERVER_CONFIG_BACKUP = WEBSERVER_CONFIG + '.backup'
    COMPOSER_WEBSERVER_CONFIG = os.path.join(CURRENT_DIRECTORY, "../../airflow/composer/webserver_config.py")

    @classmethod
    def setUpClass(cls):
        # Override webserver config with Composer specific.
        shutil.copy(WEBSERVER_CONFIG, cls.WEBSERVER_CONFIG_BACKUP)
        shutil.copy(cls.COMPOSER_WEBSERVER_CONFIG, WEBSERVER_CONFIG)
        with conf_vars(
            {
                ("webserver", "rbac_user_registration_role"): "Viewer",
                ("webserver", "rbac_autoregister_per_folder_roles"): "True",
            }
        ):
            cls.app = app.create_app(testing=True)
        cls.sm = cls.app.appbuilder.sm  # pylint: disable=no-member

    @classmethod
    def tearDownClass(cls):
        # Return back original webserver config.
        shutil.copy(cls.WEBSERVER_CONFIG_BACKUP, WEBSERVER_CONFIG)

    def get_random_id(self):
        return ''.join(random.choice(string.ascii_letters) for _ in range(10))

    def setUp(self):
        self.client = self.app.test_client()

    def test_login_no_post(self):
        resp = self.client.post("/login/")
        assert resp.status_code == 405

    def test_login_incorrect_jwt(self):
        resp = self.client.get("/login/")
        assert resp.get_data() == b"Not authorized or account inactive"
        assert resp.status_code == 403

    @mock.patch("airflow.composer.security_manager.id_token", autospec=True)
    @conf_vars({("webserver", "google_oauth2_audience"): "audience"})
    def test_login_user_auto_registered(self, id_token_mock):
        username = f"test-{self.get_random_id()}"
        email = f"test-{self.get_random_id()}@test.com"

        def id_token_mock_verify_token_side_effect(id_token, request, audience, certs_url):
            assert id_token == "jwt-test"
            assert isinstance(request, requests.Request)
            assert audience == "audience"
            assert certs_url == "https://www.gstatic.com/iap/verify/public_key"
            return {
                "sub": username,
                "email": email,
            }

        id_token_mock.verify_token.side_effect = id_token_mock_verify_token_side_effect

        resp = self.client.get("/login/", headers={"X-Goog-IAP-JWT-Assertion": "jwt-test"})

        assert resp.headers["Location"] == "/"
        assert resp.status_code == 302
        assert self.sm.find_user(username=username).roles == [self.sm.find_role(name="Viewer")]

        # Test already logged in.
        resp = self.client.get("/login/", headers={"X-Goog-IAP-JWT-Assertion": "jwt-test"})

        assert resp.headers["Location"] == "/"
        assert resp.status_code == 302

        # Test next parameter.
        self.client.get("/logout/")
        resp = self.client.get(
            "/login/?next=http%3A%2F%2Flocalhost%2Faaa", headers={"X-Goog-IAP-JWT-Assertion": "jwt-test"}
        )

        assert resp.headers["Location"] == "http://localhost/aaa"
        assert resp.status_code == 302

        # Test login user with existing email (registered above) but wrong username.
        def id_token_mock_verify_token_side_effect(
            id_token, request, audience, certs_url
        ):  # pylint: disable=function-redefined
            assert id_token == "jwt-test"
            assert isinstance(request, requests.Request)
            assert audience == "audience"
            assert certs_url == "https://www.gstatic.com/iap/verify/public_key"
            return {
                "sub": "wrong-username",
                "email": email,
            }

        id_token_mock.verify_token.side_effect = id_token_mock_verify_token_side_effect

        self.client.get("/logout/")
        resp = self.client.get("/login/", headers={"X-Goog-IAP-JWT-Assertion": "jwt-test"})

        assert resp.get_data() == b"Not authorized or account inactive"
        assert resp.status_code == 403

        # Test login user with invalid token.
        def id_token_mock_verify_token_side_effect(
            id_token, request, audience, certs_url
        ):  # pylint: disable=function-redefined
            raise ValueError("Invalid token")

        id_token_mock.verify_token.side_effect = id_token_mock_verify_token_side_effect

        self.client.get("/login/")
        resp = self.client.get("/login/", headers={"X-Goog-IAP-JWT-Assertion": "invalid-token"})

        assert resp.get_data() == b"Not authorized or account inactive"
        assert resp.status_code == 403

    @mock.patch("airflow.composer.security_manager.id_token", autospec=True)
    @conf_vars({("webserver", "google_oauth2_audience"): "audience"})
    def test_login_user_preregistered(self, id_token_mock):
        username = f"test-{self.get_random_id()}"
        email = f"test-{self.get_random_id()}@test.com"

        # Preregister user.
        create_user(self.app, username=email, role_name="Test")
        assert self.sm.find_user(username=email)

        def id_token_mock_verify_token_side_effect(id_token, request, audience, certs_url):
            assert id_token == "jwt-test"
            assert isinstance(request, requests.Request)
            assert audience == "audience"
            assert certs_url == "https://www.gstatic.com/iap/verify/public_key"
            return {
                "sub": username,
                "email": email,
            }

        id_token_mock.verify_token.side_effect = id_token_mock_verify_token_side_effect

        resp = self.client.get("/login/", headers={"X-Goog-IAP-JWT-Assertion": "jwt-test"})

        assert not self.sm.find_user(username=email)
        assert self.sm.find_user(username=username)
        assert resp.headers["Location"] == "/"
        assert resp.status_code == 302

    @mock.patch("airflow.composer.security_manager.auth.default", autospec=True)
    @mock.patch("airflow.composer.security_manager.AuthorizedSession", autospec=True)
    @conf_vars({("webserver", "jwt_public_key_url"): "jwt-public-key-url-test"})
    @conf_vars({("webserver", "inverting_proxy_backend_id"): "inverting-proxy-backend-id-test"})
    def test_login_user_auto_registered_inverting_proxy(self, authorized_session_mock, auth_default_mock):
        with open(os.path.join(self.CURRENT_DIRECTORY, 'test_data/jwtRS256.key.pub')) as f:
            public_key = f.read()

        def auth_default_mock_side_effect(scopes):
            assert scopes == ["https://www.googleapis.com/auth/cloud-platform"]
            return "credentials", "project"

        def request_side_effect(method, url, headers):
            assert method == "GET"
            assert url == "jwt-public-key-url-test"
            assert headers == {"X-Inverting-Proxy-Backend-ID": "inverting-proxy-backend-id-test"}
            return mock.Mock(status_code=200, text=public_key)

        def authorized_session_mock_side_effect(credentials):
            assert credentials == "credentials"
            return mock.Mock(request=mock.Mock(side_effect=request_side_effect))

        def request_side_effect_400_status(method, url, headers):
            assert method == "GET"
            assert url == "jwt-public-key-url-test"
            assert headers == {"X-Inverting-Proxy-Backend-ID": "inverting-proxy-backend-id-test"}
            return mock.Mock(status_code=400, text=public_key)

        def authorized_session_mock_side_effect_400_status(credentials):
            assert credentials == "credentials"
            return mock.Mock(request=mock.Mock(side_effect=request_side_effect_400_status))

        auth_default_mock.side_effect = auth_default_mock_side_effect

        first_party_token_decoded_dict = {
            "sub": f"test-{self.get_random_id()}",
            "email": f"test-{self.get_random_id()}@test.com"
        }
        byoid_subject = "subject@test.com"
        byoid_workforce_pool_name = "(global/IDPool/mynamespace)"
        byoid_token_decoded_dict = {
            "sub": f"test-{self.get_random_id()}",
            "principal": f"IDPool/mynamespace/provider/123/subject/{self.get_random_id()}",
            "display_username": f"{byoid_subject} {byoid_workforce_pool_name}"
        }
        for token_dict, email_or_principal, first_name, last_name in [
            (
                first_party_token_decoded_dict,
                first_party_token_decoded_dict["email"],
                first_party_token_decoded_dict["email"],
                "-",
            ),
            (
                byoid_token_decoded_dict,
                byoid_token_decoded_dict["principal"],
                byoid_subject,
                byoid_workforce_pool_name,
            ),
        ]:
            with open(os.path.join(self.CURRENT_DIRECTORY, 'test_data/jwtRS256.key')) as f:
                private_key = f.read()
                inv_proxy_user_id = jwt.encode(token_dict, private_key, algorithm="RS256")

            authorized_session_mock.side_effect = authorized_session_mock_side_effect

            # Test auto-registration of new user.
            resp = self.client.get("/login/", headers={"X-Inverting-Proxy-User-ID": inv_proxy_user_id})

            assert resp.headers["Location"] == "/"
            assert resp.status_code == 302
            user = self.sm.find_user(username=token_dict["sub"])
            assert user.email == email_or_principal
            assert user.first_name == first_name
            assert user.last_name == last_name
            assert user.roles == [self.sm.find_role(name="Viewer")]

            # Test already logged in.
            resp = self.client.get("/login/", headers={"X-Inverting-Proxy-User-ID": inv_proxy_user_id})

            assert resp.headers["Location"] == "/"
            assert resp.status_code == 302

            # Test invalid token.
            self.client.get("/logout/")
            resp = self.client.get("/login/", headers={"X-Inverting-Proxy-User-ID": "invalid-token"})

            assert resp.get_data() == b"Not authorized or account inactive"
            assert resp.status_code == 403

            # Test unsuccessful response from public key endpoint.
            authorized_session_mock.side_effect = authorized_session_mock_side_effect_400_status

            self.client.get("/logout/")
            resp = self.client.get("/login/", headers={"X-Inverting-Proxy-User-ID": inv_proxy_user_id})

            assert resp.get_data() == b"Not authorized or account inactive"
            assert resp.status_code == 403

    @mock.patch("airflow.composer.security_manager.auth.default", autospec=True)
    @mock.patch("airflow.composer.security_manager.AuthorizedSession", autospec=True)
    @conf_vars({("webserver", "jwt_public_key_url"): "jwt-public-key-url-test"})
    @conf_vars({("webserver", "inverting_proxy_backend_id"): "inverting-proxy-backend-id-test"})
    def test_login_user_preregistered_inverting_proxy(self, authorized_session_mock, auth_default_mock):
        with open(os.path.join(self.CURRENT_DIRECTORY, 'test_data/jwtRS256.key.pub')) as f:
            public_key = f.read()

        def request_side_effect(method, url, headers):
            assert method == "GET"
            assert url == "jwt-public-key-url-test"
            assert headers == {"X-Inverting-Proxy-Backend-ID": "inverting-proxy-backend-id-test"}
            return mock.Mock(status_code=200, text=public_key)

        def auth_default_mock_side_effect(scopes):
            assert scopes == ["https://www.googleapis.com/auth/cloud-platform"]
            return "credentials", "project"

        def authorized_session_mock_side_effect(credentials):
            assert credentials == "credentials"
            return mock.Mock(request=mock.Mock(side_effect=request_side_effect))

        auth_default_mock.side_effect = auth_default_mock_side_effect
        authorized_session_mock.side_effect = authorized_session_mock_side_effect

        first_party_token_decoded_dict = {
            "sub": f"test-{self.get_random_id()}",
            "email": f"test-{self.get_random_id()}@test.com"
        }
        byoid_token_decoded_dict = {
            "sub": f"test-{self.get_random_id()}",
            "principal": f"IDPool/mynamespace/provider/123/subject/{self.get_random_id()}",
            "display_username": "subject@test.com (global/IDPool/mynamespace)"
        }
        for token_dict, email_or_principal in [
            (first_party_token_decoded_dict, first_party_token_decoded_dict["email"]),
            (byoid_token_decoded_dict, byoid_token_decoded_dict["principal"]),
        ]:
            # Preregister user.
            create_user(self.app, username=email_or_principal, role_name="Test")
            assert self.sm.find_user(username=email_or_principal)

            with open(os.path.join(self.CURRENT_DIRECTORY, 'test_data/jwtRS256.key')) as f:
                private_key = f.read()
                inv_proxy_user_id = jwt.encode(token_dict, private_key, algorithm="RS256")

            resp = self.client.get("/login/", headers={"X-Inverting-Proxy-User-ID": inv_proxy_user_id})

            assert resp.headers["Location"] == "/"
            assert resp.status_code == 302
            assert not self.sm.find_user(username=email_or_principal)
            user = self.sm.find_user(username=token_dict["sub"])
            assert user is not None
            # first_name and last_name should not be overwritten.
            assert user.first_name == email_or_principal
            assert user.last_name == email_or_principal
            self.client.get("/logout/")

    def test_user_no_dags_role(self):
        self.assertIn(
            {
                "role": "UserNoDags",
                "perms": [
                    p
                    for p in self.sm.VIEWER_PERMISSIONS + self.sm.USER_PERMISSIONS
                    if p[1] != permissions.RESOURCE_DAG
                ],
            },
            self.sm.ROLE_CONFIGS,
        )

    def test_get_first_and_last_name(self):
        for display_username, email_or_principal, expected_first_name, expected_last_name in [
            (
                "alice.smith@example.com (global/workforcePools/example-com-employees)",
                "workforcePools/example-com-employees/provider/123/subject/alice.smith@example.com",
                "alice.smith@example.com",
                "(global/workforcePools/example-com-employees)",
            ),
            (
                "The One Eyed Raven (global/workforcePools/mystery-readers)",
                "workforcePools/mystery-readers/provider/123/subject/The One Eyed Raven",
                "The One Eyed Raven",
                "(global/workforcePools/mystery-readers)",
            ),
            ("", "alice.smith@example.com", "alice.smith@example.com", "-"),
            (
                "unexpected@username",
                "workforcePools/example-com-employees/provider/123/subject/alice.smith@example.com",
                "unexpected@username",
                "-",
            ),
            (
                "(unexpected@username)",
                "workforcePools/example-com-employees/provider/123/subject/alice.smith@example.com",
                "(unexpected@username)",
                "-",
            ),
        ]:
            first_name, last_name = _get_first_and_last_name(display_username, email_or_principal)
            assert first_name == expected_first_name
            assert last_name == expected_last_name
