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

from flask_appbuilder.const import AUTH_REMOTE_USER

from airflow.composer.patches.core.configuration import ALL_RBAC_ROLES_EXTRA_PERMISSIONS
from airflow.composer.patches.rbac.composer_auth_remote_user_view import ComposerAuthRemoteUserView
from airflow.composer.patches.rbac.per_folder_roles_autoregistration import RBAC_AUTOREGISTER_PER_FOLDER_ROLES
from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride
from airflow.providers.fab.www.security import permissions


class ComposerAirflowSecurityManager(FabAirflowSecurityManagerOverride):
    """FAB security manager adjusted per Composer needs."""

    authremoteuserview = ComposerAuthRemoteUserView

    def _init_config(self):
        app = self.appbuilder.get_app
        app.config["AUTH_TYPE"] = AUTH_REMOTE_USER

        if RBAC_AUTOREGISTER_PER_FOLDER_ROLES:
            # Add a role with permissions like in the User role except for permissions to any DAGs. This role
            # can be used as the user registration role so that new users can open Airflow UI but don't have
            # access to any DAGs by default.
            self.ROLE_CONFIGS.append(
                {
                    "role": "UserNoDags",
                    "perms": [
                        p
                        for p in self.VIEWER_PERMISSIONS + self.USER_PERMISSIONS
                        if p[1] != permissions.RESOURCE_DAG
                    ],
                }
            )

        super()._init_config()

    def sync_roles(self):
        super().sync_roles()

        # Add ALL_RBAC_ROLES_EXTRA_PERMISSIONS to all RBAC roles.
        all_roles = [role for role in self.get_all_roles()]
        for perm_tuple in ALL_RBAC_ROLES_EXTRA_PERMISSIONS:
            permission = self.create_permission(*perm_tuple)
            for role in all_roles:
                self.add_permission_to_role(role, permission)
        self.get_session.commit()
