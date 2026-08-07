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
from __future__ import annotations

import contextlib
import os
import random
import shutil
import string
from unittest import mock

import jwt
import pytest
from google.auth.transport import requests
from sqlalchemy import select

from airflow.composer.composer_airflow_rbac_bindings import USER_PERMISSIONS_TO_REMOVE, Binding
from airflow.composer.security_manager import ComposerAirflowSecurityManager, _get_first_and_last_name
from airflow.configuration import (
    WEBSERVER_CONFIG,
    initialize_config,
    write_default_airflow_configuration_if_needed,
    write_webserver_configuration_if_needed,
)
from airflow.exceptions import AirflowException
from airflow.security import permissions
from airflow.www import app
from tests.test_utils.api_connexion_utils import create_user
from tests.test_utils.config import conf_vars


@contextlib.contextmanager
def mock_rbac_bindings(bindings_list):
    if bindings_list is None:
        parsed_bindings = None
    else:
        parsed_bindings = [Binding(**b) for b in bindings_list]

    with mock.patch(
        "airflow.composer.security_manager.RBAC_BINDINGS",
        parsed_bindings,
    ):
        yield


class TestBase:
    CURRENT_DIRECTORY = os.path.dirname(os.path.realpath(__file__))
    WEBSERVER_CONFIG_BACKUP = WEBSERVER_CONFIG + ".backup"
    COMPOSER_WEBSERVER_CONFIG = os.path.join(CURRENT_DIRECTORY, "../../airflow/composer/webserver_config.py")

    @classmethod
    def setup_class(cls):
        # Initialize default webserver config
        conf = write_default_airflow_configuration_if_needed()
        write_webserver_configuration_if_needed(conf)
        initialize_config()
        # Override webserver config with Composer specific.
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
    def teardown_class(cls):
        # Delete original webserver config
        os.remove(WEBSERVER_CONFIG)

    def get_random_id(self):
        return "".join(random.choice(string.ascii_letters) for _ in range(10))

    def setup_method(self):
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
        def id_token_mock_verify_token_side_effect(id_token, request, audience, certs_url):  # pylint: disable=function-redefined
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
        def id_token_mock_verify_token_side_effect(id_token, request, audience, certs_url):  # pylint: disable=function-redefined
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
    @conf_vars({("webserver", "jwt_public_keys_url"): "jwt-public-keys-url-test"})
    @conf_vars({("webserver", "inverting_proxy_backend_id"): "inverting-proxy-backend-id-test"})
    def test_login_user_auto_registered_inverting_proxy(self, authorized_session_mock, auth_default_mock):
        # The first public key doesn't match the private key, the second matches it.
        with open(os.path.join(self.CURRENT_DIRECTORY, "test_data/jwtRS256.keys.pub")) as f:
            public_keys = f.read()

        def auth_default_mock_side_effect(scopes):
            assert scopes == ["https://www.googleapis.com/auth/cloud-platform"]
            return "credentials", "project"

        def request_side_effect(method, url, headers):
            assert method == "GET"
            assert url == "jwt-public-keys-url-test"
            assert headers == {"X-Inverting-Proxy-Backend-ID": "inverting-proxy-backend-id-test"}
            return mock.Mock(status_code=200, text=public_keys)

        def authorized_session_mock_side_effect(credentials):
            assert credentials == "credentials"
            return mock.Mock(request=mock.Mock(side_effect=request_side_effect))

        def request_side_effect_400_status(method, url, headers):
            assert method == "GET"
            assert url == "jwt-public-keys-url-test"
            assert headers == {"X-Inverting-Proxy-Backend-ID": "inverting-proxy-backend-id-test"}
            return mock.Mock(status_code=400, text=public_keys)

        def authorized_session_mock_side_effect_400_status(credentials):
            assert credentials == "credentials"
            return mock.Mock(request=mock.Mock(side_effect=request_side_effect_400_status))

        auth_default_mock.side_effect = auth_default_mock_side_effect

        first_party_token_decoded_dict = {
            "sub": f"test-{self.get_random_id()}",
            "email": f"test-{self.get_random_id()}@test.com",
        }
        byoid_subject = "subject@test.com"
        byoid_workforce_pool_name = "(global/IDPool/mynamespace)"
        byoid_token_decoded_dict = {
            "sub": f"test-{self.get_random_id()}",
            "principal": f"IDPool/mynamespace/provider/123/subject/{self.get_random_id()}",
            "display_username": f"{byoid_subject} {byoid_workforce_pool_name}",
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
            with open(os.path.join(self.CURRENT_DIRECTORY, "test_data/jwtRS256.key")) as f:
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
    @conf_vars({("webserver", "jwt_public_keys_url"): "jwt-public-keys-url-test"})
    @conf_vars({("webserver", "inverting_proxy_backend_id"): "inverting-proxy-backend-id-test"})
    def test_login_user_preregistered_inverting_proxy(self, authorized_session_mock, auth_default_mock):
        # The first public key doesn't match the private key, the second matches it.
        with open(os.path.join(self.CURRENT_DIRECTORY, "test_data/jwtRS256.keys.pub")) as f:
            public_keys = f.read()

        def request_side_effect(method, url, headers):
            assert method == "GET"
            assert url == "jwt-public-keys-url-test"
            assert headers == {"X-Inverting-Proxy-Backend-ID": "inverting-proxy-backend-id-test"}
            return mock.Mock(status_code=200, text=public_keys)

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
            "email": f"test-{self.get_random_id()}@test.com",
        }
        byoid_token_decoded_dict = {
            "sub": f"test-{self.get_random_id()}",
            "principal": f"IDPool/mynamespace/provider/123/subject/{self.get_random_id()}",
            "display_username": "subject@test.com (global/IDPool/mynamespace)",
        }
        for token_dict, email_or_principal in [
            (first_party_token_decoded_dict, first_party_token_decoded_dict["email"]),
            (byoid_token_decoded_dict, byoid_token_decoded_dict["principal"]),
        ]:
            # Preregister user.
            create_user(self.app, username=email_or_principal, role_name="Test")
            assert self.sm.find_user(username=email_or_principal)

            with open(os.path.join(self.CURRENT_DIRECTORY, "test_data/jwtRS256.key")) as f:
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
        assert {
            "role": "UserNoDags",
            "perms": [
                p
                for p in self.sm.VIEWER_PERMISSIONS + self.sm.USER_PERMISSIONS
                if p[1] != permissions.RESOURCE_DAG
            ],
        } in self.sm.ROLE_CONFIGS

    def _has_composer_menu_access(self, role_name):
        expected_permissions = [
            ("menu_access", "Composer Menu"),
            ("menu_access", "DAGs in Cloud Console"),
            ("menu_access", "DAGs in Cloud Storage"),
            ("menu_access", "Environment Monitoring"),
            ("menu_access", "Environment Logs"),
            ("menu_access", "Composer Documentation"),
        ]
        role = self.sm.find_role(role_name)
        role_permissions = [
            (permission.action.name, permission.resource.name) for permission in role.permissions
        ]
        for expected_permission in expected_permissions:
            assert expected_permission in role_permissions

    @pytest.mark.parametrize("role", ["Admin", "Viewer", "User", "Op", "Public"])
    def test_composer_menu_access(self, role):
        self._has_composer_menu_access(role)

    def test_composer_menu_access_custom_role(self):
        self.sm.add_role("custom_role")

        self.sm.sync_roles()

        self._has_composer_menu_access("custom_role")

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

    @mock.patch("airflow.composer.security_manager.ComposerAirflowSecurityManager._remove_user_permissions")
    def test_sync_roles_calls_remove_user_permissions(self, remove_permissions_mock):
        bindings = [{"role": "Viewer", "members": ["user:test@example.com"]}]
        with mock_rbac_bindings(bindings):
            self.sm.sync_roles()
            remove_permissions_mock.assert_called_once()

    def test_reconcile_user_roles(self):
        self.sm.add_role("TestRole1")
        self.sm.add_role("TestRole2")
        self.sm.add_role("TestRole3")
        user_mock = mock.Mock()
        user_mock.id = 9999
        user_mock.is_authenticated = True
        user_mock.email = "test-user@gmail.com"
        db_role = self.sm.find_role("TestRole3")
        user_mock.roles = [db_role]
        bindings = [
            {"role": "TestRole1", "members": ["user:test-user@gmail.com"]},
            {"role": "TestRole2", "members": ["group:group-2@google.com"]},
        ]
        with mock_rbac_bindings(bindings):
            with mock.patch.object(self.sm.get_session, "get", return_value=user_mock):
                with mock.patch.object(self.sm, "update_user") as update_user_mock:
                    resolved_user = self.sm.reconcile_user_roles(
                        user_mock, ["group-1@google.com", "group-2@google.com"]
                    )
                    update_user_mock.assert_called_once_with(user_mock)
        resolved_role_names = {r.name for r in resolved_user.roles}
        assert "TestRole1" in resolved_role_names
        assert "TestRole2" in resolved_role_names
        assert "TestRole3" not in resolved_role_names
        assert len(resolved_role_names) == 2

    def test_reconcile_user_roles_no_bindings_match_strips_existing_roles(self):
        self.sm.add_role("TestRole")
        db_role = self.sm.find_role("TestRole")
        user_mock = mock.Mock()
        user_mock.id = 9999
        user_mock.is_authenticated = True
        user_mock.email = "unmapped@gmail.com"
        user_mock.roles = [db_role]
        bindings = [
            {"role": "TestRole1", "members": ["user:test-user@gmail.com"]},
            {"role": "TestRole2", "members": ["group:group-2@google.com"]},
        ]
        with mock_rbac_bindings(bindings):
            with mock.patch.object(self.sm.get_session, "get", return_value=user_mock):
                with mock.patch.object(self.sm, "update_user") as update_user_mock:
                    resolved_user = self.sm.reconcile_user_roles(user_mock, ["unmapped-group@gmail.com"])
                    update_user_mock.assert_called_once_with(user_mock)
        assert resolved_user.roles == []

    def test_reconcile_user_roles_retries_on_update_user_failure(self):
        self.sm.add_role("TestRole1")
        user_mock = mock.Mock()
        user_mock.id = 9999
        user_mock.is_authenticated = True
        user_mock.email = "test-user@gmail.com"
        user_mock.roles = []
        bindings = [
            {"role": "TestRole1", "members": ["user:test-user@gmail.com"]},
        ]

        def update_user_side_effect(user):
            if update_user_mock.call_count == 1:
                user.roles = []
                return False
            return True

        with mock_rbac_bindings(bindings):
            with mock.patch.object(self.sm.get_session, "get", return_value=user_mock):
                with mock.patch.object(
                    self.sm, "update_user", side_effect=update_user_side_effect
                ) as update_user_mock:
                    resolved_user = self.sm.reconcile_user_roles(user_mock, [])
                    assert update_user_mock.call_count == 2
                    assert resolved_user is not None

    def test_remove_user_permissions(self):
        bindings = [{"role": "Viewer", "members": ["user:test@example.com"]}]

        with mock_rbac_bindings(bindings):

            def _get_user_permissions():
                return self.sm.get_session.scalars(
                    select(self.sm.permission_model)
                    .join(self.sm.action_model)
                    .join(self.sm.resource_model)
                    .where(
                        self.sm.action_model.name.in_(USER_PERMISSIONS_TO_REMOVE),
                        self.sm.resource_model.name == permissions.RESOURCE_USER,
                    )
                ).all()

            assert len(_get_user_permissions()) > 0
            self.sm._remove_user_permissions()
            assert len(_get_user_permissions()) == 0

    def test_cli_user_modifications_prevented(self):
        user_mock = mock.Mock()
        bindings = [{"role": "Viewer", "members": ["user:test@example.com"]}]

        with mock_rbac_bindings(bindings):
            with mock.patch("sys.argv", ["airflow", "users", "add-role", "test", "--role", "Admin"]):
                with pytest.raises(
                    AirflowException,
                    match="The 'airflow users add-role' CLI command is disabled when Airflow RBAC configuration is enabled.",
                ):
                    self.sm.update_user(user_mock)

            with mock.patch("sys.argv", ["airflow", "users", "remove-role", "test", "--role", "Admin"]):
                with pytest.raises(
                    AirflowException,
                    match="The 'airflow users remove-role' CLI command is disabled when Airflow RBAC configuration is enabled.",
                ):
                    self.sm.update_user(user_mock)

            with mock.patch("sys.argv", ["airflow", "users", "create", "-e", "test@test.com"]):
                with pytest.raises(
                    AirflowException,
                    match="The 'airflow users create' CLI command is disabled when Airflow RBAC configuration is enabled.",
                ):
                    self.sm.add_user(
                        username="test",
                        first_name="t",
                        last_name="t",
                        email="test@test.com",
                        role=mock.Mock(),
                    )

            with mock.patch("sys.argv", ["airflow", "users", "import", "users.json"]):
                with pytest.raises(
                    AirflowException,
                    match="The 'airflow users import' CLI command is disabled when Airflow RBAC configuration is enabled.",
                ):
                    self.sm.add_user(
                        username="test",
                        first_name="t",
                        last_name="t",
                        email="test@test.com",
                        role=mock.Mock(),
                    )

                with pytest.raises(
                    AirflowException,
                    match="The 'airflow users import' CLI command is disabled when Airflow RBAC configuration is enabled.",
                ):
                    self.sm.update_user(user_mock)

    @mock.patch(
        "airflow.composer.security_manager.ComposerAirflowSecurityManager._get_groups_from_flask_request"
    )
    @mock.patch(
        "airflow.providers.fab.auth_manager.security_manager.override.FabAirflowSecurityManagerOverride.load_user"
    )
    def test_load_user(self, super_load_user_mock, get_groups_mock):
        user_mock = mock.Mock()
        super_load_user_mock.return_value = user_mock
        get_groups_mock.return_value = ["group-1@google.com"]

        with mock.patch.object(self.sm, "reconcile_user_roles") as reconcile_mock:
            reconcile_mock.return_value = user_mock

            assert self.sm.load_user("user-id") == user_mock

            super_load_user_mock.assert_called_once_with("user-id")
            get_groups_mock.assert_called_once_with()
            reconcile_mock.assert_called_once_with(user_mock, ["group-1@google.com"])

    @mock.patch(
        "airflow.providers.fab.auth_manager.security_manager.override.FabAirflowSecurityManagerOverride.update_user"
    )
    def test_update_user(self, super_update_user_mock):
        user_mock = mock.Mock()

        with mock.patch.object(self.sm, "_check_cli_user_modifications") as check_mock:
            self.sm.update_user(user_mock)

            check_mock.assert_called_once_with()
            super_update_user_mock.assert_called_once_with(user_mock)


class TestComposerAirflowSecurityManager:
    FAB_SECURITY_MANAGER_PATH = "airflow.composer.security_manager.FabAirflowSecurityManagerOverride"
    COMPOSER_SECURITY_MANAGER_PATH = "airflow.composer.security_manager.ComposerAirflowSecurityManager"

    @mock.patch(f"{COMPOSER_SECURITY_MANAGER_PATH}.sync_perm_for_dag", autospec=True)
    @mock.patch("airflow.composer.security_manager.permissions.resource_name_for_dag", autospec=True)
    @mock.patch("airflow.composer.security_manager.DagBag", autospec=True)
    @mock.patch(f"{FAB_SECURITY_MANAGER_PATH}.create_dag_specific_permissions", autospec=True)
    @mock.patch(f"{FAB_SECURITY_MANAGER_PATH}.__init__", autospec=True)
    @conf_vars({("webserver", "rbac_autoregister_per_folder_roles"): "True"})
    def test_create_dag_specific_permissions(
        self,
        mock_super_init,
        mock_super_create_dag_specific_permissions,
        mock_dag_bag_cls,
        mock_resource_name_for_dag,
        mock_sync_from_dag,
    ):
        test_dag_id_0, test_dag_id_1 = "test-dag-id-0", "test-dag-id-1"
        test_dag_resource_name_0, test_dag_resource_name_1 = "test-resource-name-0", "test-resource-name-1"
        test_access_control_0, test_access_control_1 = mock.MagicMock(), mock.MagicMock()
        mock_dag_bag = mock.MagicMock(
            dags=mock.MagicMock(
                values=mock.MagicMock(
                    return_value=[
                        mock.MagicMock(
                            parent_dag=mock.MagicMock(dag_id=test_dag_id_0),
                            access_control=test_access_control_0,
                        ),
                        mock.MagicMock(
                            parent_dag=None, dag_id=test_dag_id_1, access_control=test_access_control_1
                        ),
                    ]
                )
            )
        )
        mock_dag_bag_cls.return_value = mock_dag_bag
        mock_resource_name_for_dag.side_effect = [test_dag_resource_name_0, test_dag_resource_name_1]
        mock_app_builder = mock.MagicMock()

        view = ComposerAirflowSecurityManager(appbuilder=mock_app_builder)
        view.create_dag_specific_permissions()

        mock_super_init.assert_called_once_with(view, mock_app_builder)
        mock_super_create_dag_specific_permissions.assert_called_once_with(view)

        mock_dag_bag_cls.assert_called_once_with(read_dags_from_db=True)
        mock_dag_bag.collect_dags_from_db.assert_called_once()
        mock_dag_bag.dags.values.assert_called_once()
        mock_resource_name_for_dag.assert_has_calls(
            [
                mock.call(test_dag_id_0),
                mock.call(test_dag_id_1),
            ]
        )
        mock_sync_from_dag.assert_has_calls(
            [
                mock.call(view, test_dag_resource_name_0, test_access_control_0),
                mock.call(view, test_dag_resource_name_1, test_access_control_1),
            ]
        )

    @mock.patch(f"{COMPOSER_SECURITY_MANAGER_PATH}.sync_perm_for_dag", autospec=True)
    @mock.patch("airflow.composer.security_manager.permissions.resource_name_for_dag", autospec=True)
    @mock.patch("airflow.composer.security_manager.DagBag", autospec=True)
    @mock.patch(f"{FAB_SECURITY_MANAGER_PATH}.create_dag_specific_permissions", autospec=True)
    @mock.patch(f"{FAB_SECURITY_MANAGER_PATH}.__init__", autospec=True)
    @conf_vars({("webserver", "rbac_autoregister_per_folder_roles"): "False"})
    def test_create_dag_specific_permissions_no_rbac_autoregister_per_folder_roles(
        self,
        mock_super_init,
        mock_super_create_dag_specific_permissions,
        mock_dag_bag_cls,
        mock_resource_name_for_dag,
        mock_sync_from_dag,
    ):
        mock_app_builder = mock.MagicMock()

        view = ComposerAirflowSecurityManager(appbuilder=mock_app_builder)
        view.create_dag_specific_permissions()

        mock_super_init.assert_called_once_with(view, mock_app_builder)
        mock_super_create_dag_specific_permissions.assert_called_once_with(view)
        mock_dag_bag_cls.assert_not_called()
        mock_sync_from_dag.assert_not_called()
        mock_resource_name_for_dag.assert_not_called()

    @mock.patch(f"{COMPOSER_SECURITY_MANAGER_PATH}._sync_dag_view_permissions", autospec=True)
    @mock.patch("airflow.composer.security_manager.permissions.resource_name_for_dag", autospec=True)
    @mock.patch(f"{FAB_SECURITY_MANAGER_PATH}.sync_perm_for_dag", autospec=True)
    @mock.patch(f"{FAB_SECURITY_MANAGER_PATH}.__init__", autospec=True)
    @conf_vars({("webserver", "rbac_autoregister_per_folder_roles"): "True"})
    def test_sync_perm_for_dag(
        self,
        mock_super_init,
        mock_super_sync_perm_for_dag,
        mock_resource_name_for_dag,
        mock_sync_dag_view_permissions,
    ):
        test_dag_id, test_dag_resource_name = "test-dag-id", "test-resource-name"
        mock_resource_name_for_dag.return_value = test_dag_resource_name
        mock_access_control, mock_app_builder = mock.MagicMock(), mock.MagicMock()

        view = ComposerAirflowSecurityManager(appbuilder=mock_app_builder)
        view.sync_perm_for_dag(dag_id=test_dag_id, access_control=mock_access_control)

        mock_super_init.assert_called_once_with(view, mock_app_builder)
        mock_super_sync_perm_for_dag.assert_called_once_with(
            view, dag_id=test_dag_id, access_control=mock_access_control
        )
        mock_resource_name_for_dag.assert_called_once_with(test_dag_id)
        mock_sync_dag_view_permissions.assert_called_once_with(
            view, test_dag_resource_name, mock_access_control
        )

    @mock.patch(f"{COMPOSER_SECURITY_MANAGER_PATH}._sync_dag_view_permissions", autospec=True)
    @mock.patch("airflow.composer.security_manager.permissions.resource_name_for_dag", autospec=True)
    @mock.patch(f"{FAB_SECURITY_MANAGER_PATH}.sync_perm_for_dag", autospec=True)
    @mock.patch(f"{FAB_SECURITY_MANAGER_PATH}.__init__", autospec=True)
    @conf_vars({("webserver", "rbac_autoregister_per_folder_roles"): "False"})
    def test_sync_perm_for_dag_no_rbac_autoregister_per_folder_roles(
        self,
        mock_super_init,
        mock_super_sync_perm_for_dag,
        mock_resource_name_for_dag,
        mock_sync_dag_view_permissions,
    ):
        test_dag_id = "test-dag-id"
        mock_access_control, mock_app_builder = mock.MagicMock(), mock.MagicMock()

        view = ComposerAirflowSecurityManager(appbuilder=mock_app_builder)
        view.sync_perm_for_dag(dag_id=test_dag_id, access_control=mock_access_control)

        mock_super_init.assert_called_once_with(view, mock_app_builder)
        mock_super_sync_perm_for_dag.assert_called_once_with(
            view, dag_id=test_dag_id, access_control=mock_access_control
        )
        mock_resource_name_for_dag.assert_not_called()
        mock_sync_dag_view_permissions.assert_not_called()
