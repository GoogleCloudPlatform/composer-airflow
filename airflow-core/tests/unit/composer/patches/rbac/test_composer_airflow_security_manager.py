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

import contextlib
import random
import string
from unittest import mock

import pytest
from flask_appbuilder.const import AUTH_REMOTE_USER
from sqlalchemy import select

from airflow.composer.patches.core.configuration import ALL_RBAC_ROLES_EXTRA_PERMISSIONS
from airflow.composer.patches.rbac.composer_airflow_rbac_bindings import USER_PERMISSIONS_TO_REMOVE, Binding
from airflow.composer.patches.rbac.composer_airflow_security_manager import ComposerAirflowSecurityManager
from airflow.composer.patches.rbac.composer_auth_remote_user_view import ComposerAuthRemoteUserView
from airflow.exceptions import AirflowException
from airflow.providers.fab.www.app import create_app
from airflow.providers.fab.www.security import permissions

from tests_common.test_utils.config import conf_vars


@contextlib.contextmanager
def mock_rbac_bindings(bindings_list):
    if bindings_list is None:
        parsed_bindings = None
    else:
        parsed_bindings = [Binding(**b) for b in bindings_list]

    with mock.patch(
        "airflow.composer.patches.rbac.composer_airflow_security_manager.RBAC_BINDINGS",
        parsed_bindings,
    ):
        yield


class TestComposerAirflowSecurityManager:
    def test_composer_airflow_security_manager(self):
        app = create_app(enable_plugins=False)
        with app.app_context():
            ComposerAirflowSecurityManager(app.appbuilder)

        assert ComposerAirflowSecurityManager.authremoteuserview == ComposerAuthRemoteUserView
        assert app.config["AUTH_TYPE"] == AUTH_REMOTE_USER

    @mock.patch(
        "airflow.composer.patches.rbac.composer_airflow_security_manager.RBAC_AUTOREGISTER_PER_FOLDER_ROLES",
        True,
    )
    def test_composer_airflow_security_manager_pfra_enabled(self):
        app = create_app(enable_plugins=False)
        with app.app_context():
            security_manager = ComposerAirflowSecurityManager(app.appbuilder)

        found = False
        for role_config in security_manager.ROLE_CONFIGS:
            if role_config["role"] == "UserNoDags":
                found = True
                break
        # Assert that config for "UserNoDags" role is present.
        assert found
        # Assert that permissions for "UserNoDags" role are subset of Viewer and User roles permissions.
        for p in role_config["perms"]:
            assert p in security_manager.VIEWER_PERMISSIONS or p in security_manager.USER_PERMISSIONS
        # Assert that some DAGs permission is present for User role but not for "UserNoDags" role.
        assert ("can_edit", "DAGs") in security_manager.USER_PERMISSIONS
        assert ("can_edit", "DAGs") not in role_config["perms"]

    @mock.patch(
        "airflow.composer.patches.rbac.composer_airflow_security_manager.ComposerAirflowSecurityManager._remove_user_permissions"
    )
    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    def test_sync_roles(self, remove_permissions_mock):
        resource_name = "".join(random.choice(string.ascii_uppercase) for _ in range(6))
        ALL_RBAC_ROLES_EXTRA_PERMISSIONS.append(("menu_access", resource_name))
        custom_role = "".join(random.choice(string.ascii_uppercase) for _ in range(6))

        def _has_permission(sm, role_name):
            role = sm.find_role(role_name)
            for permission in role.permissions:
                if permission.action.name == "menu_access" and permission.resource.name == resource_name:
                    return True

            return False

        app = create_app(enable_plugins=False)
        app.appbuilder.sm.add_role(custom_role)

        # Built-in roles should get permission already, since sync_roles() is called during app creation.
        assert _has_permission(app.appbuilder.sm, "Admin")
        # Custom role shouldn't have permission yet, because the role was added after app created.
        assert not _has_permission(app.appbuilder.sm, custom_role)

        app.appbuilder.sm.sync_roles()

        assert _has_permission(app.appbuilder.sm, "Admin")
        assert _has_permission(app.appbuilder.sm, custom_role)
        remove_permissions_mock.assert_not_called()

    @mock.patch(
        "airflow.composer.patches.rbac.composer_airflow_security_manager.ComposerAirflowSecurityManager._remove_user_permissions"
    )
    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    def test_sync_roles_calls_remove_user_permissions(self, remove_permissions_mock):
        app = create_app(enable_plugins=False)
        sm = app.appbuilder.sm

        bindings = [{"role": "Viewer", "members": ["user:test@example.com"]}]
        with mock_rbac_bindings(bindings):
            sm.sync_roles()

            remove_permissions_mock.assert_called_once()

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    def test_find_user_by_username(self):
        app = create_app(enable_plugins=False)
        username = "".join(random.choice(string.ascii_uppercase) for _ in range(6))
        email = f"{username}@gmail.com"
        app.appbuilder.sm.add_user(
            username=username,
            first_name="first name",
            last_name="last name",
            email=email,
        )

        actual_user = app.appbuilder.sm.find_user_by_username(username)
        actual_user2 = app.appbuilder.sm.find_user_by_username(username.lower())

        assert actual_user.username == username
        assert actual_user.first_name == "first name"
        assert actual_user.last_name == "last name"
        assert actual_user.email == email
        assert actual_user2.id == actual_user.id

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    def test_reconcile_user_roles(self):
        app = create_app(enable_plugins=False)
        sm = app.appbuilder.sm
        sm.add_role("TestRole1")
        sm.add_role("TestRole2")
        sm.add_role("TestRole3")
        user_mock = mock.Mock()
        user_mock.id = 9999
        user_mock.is_authenticated = True
        user_mock.email = "test-user@gmail.com"
        db_role = sm.find_role("TestRole3")
        user_mock.roles = [db_role]
        bindings = [
            {"role": "TestRole1", "members": ["user:test-user@gmail.com"]},
            {"role": "TestRole2", "members": ["group:group-2@google.com"]},
        ]
        with mock_rbac_bindings(bindings):
            with mock.patch.object(sm.session, "get", return_value=user_mock):
                with mock.patch.object(sm, "update_user") as update_user_mock:
                    resolved_user = sm.reconcile_user_roles(
                        user_mock, ["group-1@google.com", "group-2@google.com"]
                    )
                    update_user_mock.assert_called_once_with(user_mock)
        resolved_role_names = {r.name for r in resolved_user.roles}
        assert "TestRole1" in resolved_role_names
        assert "TestRole2" in resolved_role_names
        assert "TestRole3" not in resolved_role_names
        assert len(resolved_role_names) == 2

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    def test_reconcile_user_roles_no_bindings_match_strips_existing_roles(self):
        app = create_app(enable_plugins=False)
        sm = app.appbuilder.sm
        sm.add_role("TestRole")
        db_role = sm.find_role("TestRole")
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
            with mock.patch.object(sm.session, "get", return_value=user_mock):
                with mock.patch.object(sm, "update_user") as update_user_mock:
                    resolved_user = sm.reconcile_user_roles(user_mock, ["unmapped-group@gmail.com"])
                    update_user_mock.assert_called_once_with(user_mock)
        assert resolved_user.roles == []

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    def test_reconcile_user_roles_retries_on_update_user_failure(self):
        app = create_app(enable_plugins=False)
        sm = app.appbuilder.sm
        sm.add_role("TestRole1")
        user_mock = mock.Mock()
        user_mock.id = 9999
        user_mock.is_authenticated = True
        user_mock.email = "test-user@gmail.com"
        user_mock.roles = []
        bindings = [
            {"role": "TestRole1", "members": ["user:test-user@gmail.com"]},
        ]

        def update_user_side_effect(user):
            # Simulate DB rollback on failure by resetting roles
            if update_user_mock.call_count == 1:
                user.roles = []
                return False
            return True

        with mock_rbac_bindings(bindings):
            with mock.patch.object(sm.session, "get", return_value=user_mock):
                with mock.patch.object(
                    sm, "update_user", side_effect=update_user_side_effect
                ) as update_user_mock:
                    resolved_user = sm.reconcile_user_roles(user_mock, [])
                    assert update_user_mock.call_count == 2
                    assert resolved_user is not None

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    def test_remove_user_permissions(self):
        app = create_app(enable_plugins=False)
        sm = app.appbuilder.sm
        bindings = [{"role": "Viewer", "members": ["user:test@example.com"]}]

        with mock_rbac_bindings(bindings):

            def _get_user_permissions():
                return sm.session.scalars(
                    select(sm.permission_model)
                    .join(sm.action_model)
                    .join(sm.resource_model)
                    .where(
                        sm.action_model.name.in_(USER_PERMISSIONS_TO_REMOVE),
                        sm.resource_model.name == permissions.RESOURCE_USER,
                    )
                ).all()

            assert len(_get_user_permissions()) > 0
            sm._remove_user_permissions()
            assert len(_get_user_permissions()) == 0

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    def test_cli_user_modifications_prevented(self):
        app = create_app(enable_plugins=False)
        sm = app.appbuilder.sm
        user_mock = mock.Mock()

        bindings = [{"role": "Viewer", "members": ["user:test@example.com"]}]

        with mock_rbac_bindings(bindings):
            with mock.patch("sys.argv", ["airflow", "users", "add-role", "test", "--role", "Admin"]):
                with pytest.raises(
                    AirflowException,
                    match="The 'airflow users add-role' CLI command is disabled when Airflow RBAC configuration is enabled.",
                ):
                    sm.update_user(user_mock)

            with mock.patch("sys.argv", ["airflow", "users", "remove-role", "test", "--role", "Admin"]):
                with pytest.raises(
                    AirflowException,
                    match="The 'airflow users remove-role' CLI command is disabled when Airflow RBAC configuration is enabled.",
                ):
                    sm.update_user(user_mock)

            with mock.patch("sys.argv", ["airflow", "users", "create", "-e", "test@test.com"]):
                with pytest.raises(
                    AirflowException,
                    match="The 'airflow users create' CLI command is disabled when Airflow RBAC configuration is enabled.",
                ):
                    sm.add_user(
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
                    sm.add_user(
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
                    sm.update_user(user_mock)

    @mock.patch(
        "airflow.composer.patches.rbac.composer_airflow_security_manager.ComposerAirflowSecurityManager._get_groups_from_flask_request"
    )
    @mock.patch(
        "airflow.providers.fab.auth_manager.security_manager.override.FabAirflowSecurityManagerOverride.load_user"
    )
    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    def test_load_user(self, super_load_user_mock, get_groups_mock):
        app = create_app(enable_plugins=False)
        sm = app.appbuilder.sm
        user_mock = mock.Mock()
        super_load_user_mock.return_value = user_mock
        get_groups_mock.return_value = ["group-1@google.com"]

        with mock.patch.object(sm, "reconcile_user_roles") as reconcile_mock:
            reconcile_mock.return_value = user_mock

            assert sm.load_user("user-id") == user_mock

            super_load_user_mock.assert_called_once_with("user-id")
            get_groups_mock.assert_called_once_with()
            reconcile_mock.assert_called_once_with(user_mock, ["group-1@google.com"])

    @mock.patch(
        "airflow.providers.fab.auth_manager.security_manager.override.FabAirflowSecurityManagerOverride.update_user"
    )
    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    def test_update_user(self, super_update_user_mock):
        app = create_app(enable_plugins=False)
        sm = app.appbuilder.sm
        user_mock = mock.Mock()

        with mock.patch.object(sm, "_check_cli_user_modifications") as check_mock:
            sm.update_user(user_mock)

            check_mock.assert_called_once_with()
            super_update_user_mock.assert_called_once_with(user_mock)
