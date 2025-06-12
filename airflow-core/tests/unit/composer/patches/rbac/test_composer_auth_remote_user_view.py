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

from airflow.composer.patches.rbac.composer_auth_remote_user_view import ComposerAuthRemoteUserView
from airflow.providers.fab.auth_manager.models import User
from airflow.providers.fab.www.app import create_app
from airflow.utils.session import provide_session

from tests_common.test_utils.config import conf_vars


class TestComposerAuthRemoteUserView:
    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.ComposerAuthRemoteUserView.auth_current_user",
        return_value=mock.Mock(),
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.get_flashed_messages", autospec=True
    )
    def test_login_successful(self, get_flashed_messages_mock, auth_current_user_mock):
        app = create_app(enable_plugins=False)
        client = app.test_client()

        response = client.get("/login/")

        get_flashed_messages_mock.assert_called_once_with()
        assert response.status_code == 302
        assert response.location == "/"

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.ComposerAuthRemoteUserView.auth_current_user",
        return_value=mock.Mock(),
    )
    def test_login_successful_redirect_next(self, auth_current_user_mock):
        app = create_app(enable_plugins=False)
        client = app.test_client()

        response = client.get("/login/?next=http://localhost/test")

        assert response.status_code == 302
        assert response.location == "http://localhost/test"

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.ComposerAuthRemoteUserView.auth_current_user",
        return_value=mock.Mock(),
    )
    def test_login_successful_next_unsafe_redirect_index(self, auth_current_user_mock):
        app = create_app(enable_plugins=False)
        client = app.test_client()

        response = client.get("/login/?next=http://unsafe/test")

        assert response.status_code == 302
        assert response.location == "/"

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.ComposerAuthRemoteUserView.auth_current_user",
        return_value=None,
    )
    def test_login_failed(self, auth_current_user_mock):
        app = create_app(enable_plugins=False)
        client = app.test_client()

        response = client.get("/login/")

        assert response.status_code == 403
        assert response.text == "Not authorized or account inactive"

    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.request",
        mock.Mock(
            headers={
                "X-Inverting-Proxy-User-ID": "test-user-id",
            }
        ),
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.ComposerAuthRemoteUserView._decode_inverting_proxy_jwt",
        autospec=True,
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.ComposerAuthRemoteUserView._register_user_if_needed",
        autospec=True,
    )
    @mock.patch("airflow.composer.patches.rbac.composer_auth_remote_user_view.login_user", autospec=True)
    def test_auth_current_user(
        self, login_user_mock, _register_user_if_needed_mock, _decode_inverting_proxy_jwt_mock
    ):
        user_mock = mock.Mock()
        _decode_inverting_proxy_jwt_mock.return_value = {
            "username": "test-username",
            "email": "test-email",
        }
        _register_user_if_needed_mock.return_value = user_mock
        view = ComposerAuthRemoteUserView()

        actual_user = view.auth_current_user()

        _decode_inverting_proxy_jwt_mock.assert_called_once_with(view, "test-user-id")
        _register_user_if_needed_mock.assert_called_once_with(
            view,
            username="test-username",
            email="test-email",
        )
        login_user_mock.assert_called_once_with(user_mock)
        assert actual_user == user_mock

    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.request",
        mock.Mock(headers={}),
    )
    def test_auth_current_user_no_header(self):
        view = ComposerAuthRemoteUserView()

        actual_user = view.auth_current_user()

        assert actual_user is None

    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.request",
        mock.Mock(
            headers={
                "X-Inverting-Proxy-User-ID": "test-user-id",
            }
        ),
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.ComposerAuthRemoteUserView._decode_inverting_proxy_jwt",
        return_value=None,
    )
    def test_auth_current_user_invalid_jwt(self, _decode_inverting_proxy_jwt_mock):
        view = ComposerAuthRemoteUserView()

        actual_user = view.auth_current_user()

        _decode_inverting_proxy_jwt_mock.assert_called_with("test-user-id")
        assert actual_user is None

    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.request",
        mock.Mock(
            headers={
                "X-Inverting-Proxy-User-ID": "test-user-id",
            }
        ),
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.ComposerAuthRemoteUserView._decode_inverting_proxy_jwt",
        autospec=True,
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.ComposerAuthRemoteUserView._register_user_if_needed",
        autospec=True,
    )
    def test_auth_current_user_not_registered(
        self, _register_user_if_needed_mock, _decode_inverting_proxy_jwt_mock
    ):
        _decode_inverting_proxy_jwt_mock.return_value = {
            "username": "test-username",
            "email": "test-email",
        }
        _register_user_if_needed_mock.return_value = None
        view = ComposerAuthRemoteUserView()

        actual_user = view.auth_current_user()

        _decode_inverting_proxy_jwt_mock.assert_called_with(view, "test-user-id")
        _register_user_if_needed_mock.assert_called_with(
            view,
            username="test-username",
            email="test-email",
        )
        assert actual_user is None

    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.request",
        mock.Mock(
            headers={
                "X-Inverting-Proxy-User-ID": "test-user-id",
            }
        ),
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.ComposerAuthRemoteUserView._decode_inverting_proxy_jwt",
        autospec=True,
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.ComposerAuthRemoteUserView._register_user_if_needed",
        autospec=True,
    )
    def test_auth_current_user_not_active(
        self, _register_user_if_needed_mock, _decode_inverting_proxy_jwt_mock
    ):
        user_mock = mock.Mock(is_active=False)
        _decode_inverting_proxy_jwt_mock.return_value = {
            "username": "test-username",
            "email": "test-email",
        }
        _register_user_if_needed_mock.return_value = user_mock
        view = ComposerAuthRemoteUserView()

        actual_user = view.auth_current_user()

        _decode_inverting_proxy_jwt_mock.assert_called_with(view, "test-user-id")
        _register_user_if_needed_mock.assert_called_with(
            view,
            username="test-username",
            email="test-email",
        )
        assert actual_user is None

    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.request",
        mock.Mock(
            headers={
                "X-Inverting-Proxy-User-ID": "test-user-id",
            }
        ),
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.ComposerAuthRemoteUserView._decode_inverting_proxy_jwt",
        autospec=True,
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.ComposerAuthRemoteUserView._register_user_if_needed",
        autospec=True,
    )
    @mock.patch("airflow.composer.patches.rbac.composer_auth_remote_user_view.login_user", autospec=True)
    def test_auth_current_user_retry_successful(
        self, login_user_mock, _register_user_if_needed_mock, _decode_inverting_proxy_jwt_mock
    ):
        user_mock = mock.Mock()
        _decode_inverting_proxy_jwt_mock.side_effect = [
            None,
            {
                "username": "test-username",
                "email": "test-email",
            },
        ]
        _register_user_if_needed_mock.return_value = user_mock
        view = ComposerAuthRemoteUserView()

        actual_user = view.auth_current_user()

        assert actual_user == user_mock

    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.google.auth.default",
        autospec=True,
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.AuthorizedSession",
        autospec=True,
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.JWT_PUBLIC_KEY_URL",
        "test-public-key-url",
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.INVERTING_PROXY_BACKEND_ID",
        "test-inverting-proxy-id",
    )
    def test_decode_inverting_proxy_jwt(self, authorized_session_mock, google_auth_default_mock):
        credentials_mock = mock.Mock()
        google_auth_default_mock.return_value = credentials_mock, mock.Mock()
        request_mock = mock.Mock(
            return_value=mock.Mock(
                status_code=200,
                text="""-----BEGIN PUBLIC KEY-----
MIIBITANBgkqhkiG9w0BAQEFAAOCAQ4AMIIBCQKCAQBFS07s5fq4x0xFooSb9spu
8PRBFhT1lTQo9+PBLznUVTdyPDO04eHMftgbCwAiCSWZ1COb9rTwFRkWL+TfXc2t
Upxk/l8Mb9jkJtBQ/JOFJ9jk3lZ6T0mCl7Kann+9dVC18JhIQNbke08dJWTdxxqX
8WcC++GGtBaQOrShWwQ6vnxItUeVSs/QFjKqr1KGemNhRdLqphcMoZ3UfoggYZ8p
sxusuBu42fUEP9F0rRpbV81xEmK1Ib5tdZ65mW+Dy9jjIh2nzojgXTKiXjB56vDk
N03Krbc3a4Rf9cxnGgo4gHEvY3bTb6ikqWQKJMaAtFJhz5gvXDzDHt1qso/okZE5
AgMBAAE=
-----END PUBLIC KEY-----
""",
            )
        )
        authorized_session_mock.return_value = mock.Mock(request=request_mock)
        view = ComposerAuthRemoteUserView()

        actual_result = view._decode_inverting_proxy_jwt(
            "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0LXVzZXJuYW1lIiwiZW1haWwiOiJ0ZXN0LWVtYWlsIn0"
            ".LrLq7-cR1ZULO4iDck0w4DvfVMq3la23AFhsXLzv261F5iee2cX0IHxGbt5eKqpIOQQuQkAj6YjOYJGVeA95nstN0XygfFy"
            "J_Oc1KXyJz5ExGpRx_pjLvBDcxBjr6V2mDr8ssCXeT9IKLDHJ6soEp7ReWg5BPkv_fhsoXnMfSVimjTSLZo_W8uiKtUlXaNv"
            "vvGYWYepBgAEBjz4BS_U4MoYirK1OL_vp6r0Qpsu0Bra6KTMkTm5sVtrN3gcB4XCksYUkElHjbCcrQBKnqptijhb64xHAvMC"
            "KCHaHXzVNqeOdlQK0lm72esWf1gMnKbeDopbe-SU8NRi2DKK3Q-4EKw"
        )

        google_auth_default_mock.assert_called_once_with(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        authorized_session_mock.assert_called_once_with(credentials_mock)
        request_mock.assert_called_once_with(
            "GET",
            "test-public-key-url",
            headers={
                "X-Inverting-Proxy-Backend-ID": "test-inverting-proxy-id",
            },
        )
        assert actual_result == {
            "username": "test-username",
            "email": "test-email",
        }

    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.google.auth.default",
        autospec=True,
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.AuthorizedSession",
        autospec=True,
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.JWT_PUBLIC_KEY_URL",
        "test-public-key-url",
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.INVERTING_PROXY_BACKEND_ID",
        "test-inverting-proxy-id",
    )
    def test_decode_inverting_proxy_jwt_principal(self, authorized_session_mock, google_auth_default_mock):
        credentials_mock = mock.Mock()
        google_auth_default_mock.return_value = credentials_mock, mock.Mock()
        request_mock = mock.Mock(
            return_value=mock.Mock(
                status_code=200,
                text="""-----BEGIN PUBLIC KEY-----
MIIBITANBgkqhkiG9w0BAQEFAAOCAQ4AMIIBCQKCAQBFS07s5fq4x0xFooSb9spu
8PRBFhT1lTQo9+PBLznUVTdyPDO04eHMftgbCwAiCSWZ1COb9rTwFRkWL+TfXc2t
Upxk/l8Mb9jkJtBQ/JOFJ9jk3lZ6T0mCl7Kann+9dVC18JhIQNbke08dJWTdxxqX
8WcC++GGtBaQOrShWwQ6vnxItUeVSs/QFjKqr1KGemNhRdLqphcMoZ3UfoggYZ8p
sxusuBu42fUEP9F0rRpbV81xEmK1Ib5tdZ65mW+Dy9jjIh2nzojgXTKiXjB56vDk
N03Krbc3a4Rf9cxnGgo4gHEvY3bTb6ikqWQKJMaAtFJhz5gvXDzDHt1qso/okZE5
AgMBAAE=
-----END PUBLIC KEY-----
""",
            )
        )
        authorized_session_mock.return_value = mock.Mock(request=request_mock)
        view = ComposerAuthRemoteUserView()

        actual_result = view._decode_inverting_proxy_jwt(
            "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0LXVzZXJuYW1lIiwicHJpbmNpcGFsIjoidGVzdC1wcml"
            "uY2lwYWwifQ.PUtKgAqCoAERFqGSnTlXY_XuhegmFbSqhWznRa28G-qKeKoZN9sJwSwW1bWDiyf3IIXatUR-3OTpkl1oMHjh"
            "3flhRX65Q5CxfPbXL2BYaCV09RrV29en2DgFu_K4ENepRZabziRvWGwkIoUIuzdcY1KAxjQoJhChlnvvl7bISgo3zse6gS81"
            "_jXkfZ3bmSFPP1KABPA_RKCb5KovZEziTTIFXrAUNsO6RzJFyFGUBey8gaQM5rbgBafalwduO1wJNnQz57to2Ca3zmoQ5X4m"
            "1THJEt-UBTOhyZ7vL3_tsJ7D2JIFET4bMGGCxL6zsntw-CNSsu16mLvyRbXPt2firg"
        )

        google_auth_default_mock.assert_called_once_with(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        authorized_session_mock.assert_called_once_with(credentials_mock)
        request_mock.assert_called_once_with(
            "GET",
            "test-public-key-url",
            headers={
                "X-Inverting-Proxy-Backend-ID": "test-inverting-proxy-id",
            },
        )
        assert actual_result == {
            "username": "test-username",
            "email": "test-principal",
        }

    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.google.auth.default",
        autospec=True,
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.AuthorizedSession",
        autospec=True,
    )
    def test_decode_inverting_proxy_jwt_public_key_not_fetched(
        self, authorized_session_mock, google_auth_default_mock
    ):
        google_auth_default_mock.return_value = mock.Mock(), mock.Mock()
        authorized_session_mock.return_value = mock.Mock(
            request=mock.Mock(return_value=mock.Mock(status_code=500))
        )
        view = ComposerAuthRemoteUserView()

        actual_result = view._decode_inverting_proxy_jwt("aaa")

        assert actual_result is None

    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.google.auth.default",
        autospec=True,
    )
    def test_decode_inverting_proxy_jwt_exception(self, google_auth_default_mock):
        google_auth_default_mock.return_value = ValueError("error")
        view = ComposerAuthRemoteUserView()

        actual_result = view._decode_inverting_proxy_jwt("aaa")

        assert actual_result is None

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.RBAC_USER_REGISTRATION_ROLE",
        "Op",
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_airflow_security_manager.ComposerAirflowSecurityManager.update_user_auth_stat",
        autospec=True,
    )
    def test_register_user_if_needed(self, update_user_auth_stat_mock):
        app = create_app(enable_plugins=False)
        view = ComposerAuthRemoteUserView()
        view.appbuilder = app.appbuilder
        username = "".join(random.choice(string.ascii_uppercase) for _ in range(6))
        email = f"{username}@google.com"

        actual_user = view._register_user_if_needed(username=username, email=email)

        update_user_auth_stat_mock.assert_called_once_with(view.appbuilder.sm, actual_user)
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
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.RBAC_USER_REGISTRATION_ROLE",
        "Op",
    )
    def test_register_user_if_needed_already_registered(self):
        app = create_app(enable_plugins=False)
        view = ComposerAuthRemoteUserView()
        view.appbuilder = app.appbuilder
        username = "".join(random.choice(string.ascii_uppercase) for _ in range(6))
        email = f"{username}@google.com"

        view._register_user_if_needed(username=username, email=email)
        # Try to register second time with same username and email.
        actual_user = view._register_user_if_needed(username=username, email=email)

        assert actual_user.username == username

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.RBAC_USER_REGISTRATION_ROLE",
        "Op",
    )
    def test_register_user_if_needed_unsuccessful(self):
        app = create_app(enable_plugins=False)
        view = ComposerAuthRemoteUserView()
        view.appbuilder = app.appbuilder
        username = "".join(random.choice(string.ascii_uppercase) for _ in range(6))
        email = f"{username}@google.com"

        view._register_user_if_needed(username=username, email=email)
        # Try to register second time with different username but same email. sm.add_user method should return
        # None in this case.
        actual_user = view._register_user_if_needed(username=username + "2", email=email)

        assert actual_user is None

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.RBAC_USER_REGISTRATION_ROLE",
        "User",
    )
    def test_register_user_if_needed_preregistered_user(self):
        app = create_app(enable_plugins=False)
        view = ComposerAuthRemoteUserView()
        view.appbuilder = app.appbuilder
        username = "".join(random.choice(string.ascii_uppercase) for _ in range(6))
        email = f"{username}@google.com"
        # Preregister user with username=email.
        preregistered_user = view._register_user_if_needed(username=email, email=email)
        preregistered_user_id = preregistered_user.id

        actual_user = view._register_user_if_needed(username=username, email=email)

        assert actual_user.id == preregistered_user_id
        assert actual_user.username == username
        assert actual_user.email == email

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.RBAC_USER_REGISTRATION_ROLE",
        "User",
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_airflow_security_manager.ComposerAirflowSecurityManager.update_user",
        autospec=True,
    )
    def test_register_user_if_needed_preregistered_user_update_fails(self, update_user_mock):
        update_user_mock.return_value = False
        app = create_app(enable_plugins=False)
        view = ComposerAuthRemoteUserView()
        view.appbuilder = app.appbuilder
        username = "".join(random.choice(string.ascii_uppercase) for _ in range(6))
        email = f"{username}@google.com"
        # Preregister user with username=email.
        view._register_user_if_needed(username=email, email=email)

        actual_user = view._register_user_if_needed(username=username, email=email)

        assert actual_user is None

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

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.ComposerAuthRemoteUserView.auth_current_user",
        return_value=mock.Mock(),
    )
    @provide_session
    def test_token_successful(self, auth_current_user_mock, session):
        app = create_app(enable_plugins=False)
        client = app.test_client()
        session_interface = app.session_interface
        session_model = session_interface.sql_session_model

        response = client.post("/token")

        assert response.status_code == 200
        assert "access_token" in response.json
        user_session = (
            session.query(session_model)
            .filter(session_model.session_id == response.json["access_token"])
            .first()
        )
        assert user_session is not None

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch(
        "airflow.composer.patches.rbac.composer_auth_remote_user_view.ComposerAuthRemoteUserView.auth_current_user",
        return_value=None,
    )
    def test_token_failed(self, auth_current_user_mock):
        app = create_app(enable_plugins=False)
        client = app.test_client()

        response = client.post("/token")

        assert response.status_code == 403
        assert response.text == "Not authorized or account inactive"
