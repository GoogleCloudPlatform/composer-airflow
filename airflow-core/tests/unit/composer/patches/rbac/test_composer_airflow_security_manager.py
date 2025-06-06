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

from flask_appbuilder.const import AUTH_REMOTE_USER

from airflow.composer.patches.core.configuration import ALL_RBAC_ROLES_EXTRA_PERMISSIONS
from airflow.composer.patches.rbac.composer_airflow_security_manager import ComposerAirflowSecurityManager
from airflow.composer.patches.rbac.composer_auth_remote_user_view import ComposerAuthRemoteUserView
from airflow.providers.fab.www.app import create_app

from tests_common.test_utils.config import conf_vars


class TestComposerAirflowSecurityManager:
    def test_composer_airflow_security_manager(self):
        app = create_app(enable_plugins=False)
        ComposerAirflowSecurityManager(app.appbuilder)

        assert ComposerAirflowSecurityManager.authremoteuserview == ComposerAuthRemoteUserView
        assert app.config["AUTH_TYPE"] == AUTH_REMOTE_USER

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    def test_sync_roles(self):
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
