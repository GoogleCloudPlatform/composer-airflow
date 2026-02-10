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

from unittest import mock

from airflow.providers.fab.www.app import create_app

from tests_common.test_utils.config import conf_vars


class TestComposerAuthRemoteUserView:
    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.decode_inverting_proxy_jwt",
        autospec=True,
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.get_or_register_user", autospec=True
    )
    @mock.patch("airflow.composer.patches.rbac.composer_auth_remote_user_view.login_user", autospec=True)
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.get_flashed_messages", autospec=True
    )
    def test_login_successful(
        self,
        get_flashed_messages_mock,
        login_user_mock,
        get_or_register_user_mock,
        decode_inverting_proxy_jwt_mock,
    ):
        app = create_app(enable_plugins=False)
        client = app.test_client()
        decode_inverting_proxy_jwt_mock.return_value = {
            "username": "test-username",
            "email": "test-email",
        }
        user_mock = mock.Mock(is_active=True)
        get_or_register_user_mock.return_value = user_mock

        response = client.get("/login/", headers={"X-Inverting-Proxy-User-ID": "test-jwt"})

        decode_inverting_proxy_jwt_mock.assert_called_once_with("test-jwt")
        get_or_register_user_mock.assert_called_once_with(
            username="test-username",
            email="test-email",
        )
        login_user_mock.assert_called_once_with(user_mock)
        get_flashed_messages_mock.assert_called_once_with()
        assert response.status_code == 302
        assert response.location == "/"

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.decode_inverting_proxy_jwt",
        autospec=True,
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.get_or_register_user", autospec=True
    )
    @mock.patch("airflow.composer.patches.rbac.composer_auth_remote_user_view.login_user", autospec=True)
    def test_login_successful_redirect_next(
        self, login_user_mock, get_or_register_user_mock, decode_inverting_proxy_jwt_mock
    ):
        app = create_app(enable_plugins=False)
        client = app.test_client()

        response = client.get("/login/?next=http://localhost/test")

        assert response.status_code == 302
        assert response.location == "http://localhost/test"

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.decode_inverting_proxy_jwt",
        autospec=True,
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.get_or_register_user", autospec=True
    )
    @mock.patch("airflow.composer.patches.rbac.composer_auth_remote_user_view.login_user", autospec=True)
    def test_login_successful_unsafe_next_redirect_index(
        self, login_user_mock, get_or_register_user_mock, decode_inverting_proxy_jwt_mock
    ):
        app = create_app(enable_plugins=False)
        client = app.test_client()

        response = client.get("/login/?next=http://unsafe/test")

        assert response.status_code == 302
        assert response.location == "/"

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.decode_inverting_proxy_jwt",
        autospec=True,
    )
    def test_login_failed_unable_to_decode_jwt(self, decode_inverting_proxy_jwt_mock):
        app = create_app(enable_plugins=False)
        client = app.test_client()
        decode_inverting_proxy_jwt_mock.return_value = None

        response = client.get("/login/", headers={"X-Inverting-Proxy-User-ID": "test-jwt"})

        assert response.status_code == 401
        assert response.text == "Not authorized - unable to decode inverting proxy JWT"

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.decode_inverting_proxy_jwt",
        autospec=True,
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.get_or_register_user", autospec=True
    )
    def test_login_failed_unable_to_register(
        self,
        get_or_register_user_mock,
        decode_inverting_proxy_jwt_mock,
    ):
        app = create_app(enable_plugins=False)
        client = app.test_client()
        get_or_register_user_mock.return_value = None

        response = client.get("/login/", headers={"X-Inverting-Proxy-User-ID": "test-jwt"})

        assert response.status_code == 401
        assert response.text == "Not authorized - unable to register or inactive user"

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.decode_inverting_proxy_jwt",
        autospec=True,
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.get_or_register_user", autospec=True
    )
    def test_login_failed_inactive_user(
        self,
        get_or_register_user_mock,
        decode_inverting_proxy_jwt_mock,
    ):
        app = create_app(enable_plugins=False)
        client = app.test_client()
        get_or_register_user_mock.return_value = mock.Mock(is_active=False)

        response = client.get("/login/", headers={"X-Inverting-Proxy-User-ID": "test-jwt"})

        assert response.status_code == 401
        assert response.text == "Not authorized - unable to register or inactive user"

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    def test_logout(self):
        app = create_app(enable_plugins=False)
        client = app.test_client()

        response = client.get("/logout/")

        assert response.status_code == 302
        assert response.location == "/"
        # Check that response has header to delete DATALAB_TUNNEL_TOKEN cookie.
        assert any(
            map(
                lambda h: h
                == (
                    "Set-Cookie",
                    "DATALAB_TUNNEL_TOKEN=; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Max-Age=0; Path=/",
                ),
                response.headers,
            )
        )
