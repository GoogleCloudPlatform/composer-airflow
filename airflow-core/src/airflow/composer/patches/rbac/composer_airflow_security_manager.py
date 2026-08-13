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

import logging
import sys

from flask import current_app, request as flask_request
from flask_appbuilder.const import AUTH_REMOTE_USER
from sqlalchemy import delete, func, select
from sqlalchemy.orm import object_session
from tenacity import retry, retry_if_result, stop_after_attempt

from airflow.composer.patches.core.configuration import ALL_RBAC_ROLES_EXTRA_PERMISSIONS
from airflow.composer.patches.rbac.composer_airflow_rbac_bindings import (
    RBAC_BINDINGS,
    USER_METHODS_TO_REMOVE,
    USER_PERMISSIONS_TO_REMOVE,
)
from airflow.composer.patches.rbac.composer_auth_remote_user_view import ComposerAuthRemoteUserView
from airflow.composer.patches.rbac.per_folder_roles_autoregistration import RBAC_AUTOREGISTER_PER_FOLDER_ROLES
from airflow.composer.patches.rbac.utils import (
    INVERTING_PROXY_USER_ID_REQUEST_HEADER,
    decode_inverting_proxy_jwt,
)
from airflow.exceptions import AirflowException
from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride
from airflow.providers.fab.www.security import permissions
from airflow.utils.session import provide_session

log = logging.getLogger(__name__)


class ComposerAirflowSecurityManager(FabAirflowSecurityManagerOverride):
    """FAB security manager adjusted per Composer needs."""

    authremoteuserview = ComposerAuthRemoteUserView

    def _init_config(self):
        current_app.config["AUTH_TYPE"] = AUTH_REMOTE_USER

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

    @staticmethod
    def _get_groups_from_flask_request() -> list[str] | None:
        """
        Extract user groups from the JWT token in request headers from flask request.

        Returns:
            A list of group names if groups are present in the valid JWT token.
            None if unable to extract groups from the token, the token is missing, or decoding failed.
        """
        jwt_token = flask_request.headers.get(INVERTING_PROXY_USER_ID_REQUEST_HEADER)
        if not jwt_token:
            return None

        decoded = decode_inverting_proxy_jwt(jwt_token)
        if not decoded:
            return None
        return decoded.get("google_groups", [])

    def _remove_user_permissions(self) -> None:
        """
        Remove ability to manually edit Users.

        If the declarative RBAC config is disabled, these permissions will be automatically recreated
        and assigned to appropriate default roles by Airflow itself during its regular RBAC sync.
        """
        self.session.execute(
            delete(self.permission_model).where(
                self.permission_model.action.has(self.action_model.name.in_(USER_PERMISSIONS_TO_REMOVE)),
                self.permission_model.resource.has(self.resource_model.name == permissions.RESOURCE_USER),
            )
        )

        self.session.commit()

    @staticmethod
    def _check_cli_user_modifications() -> None:
        if RBAC_BINDINGS:
            # Check if this is being executed from the Airflow CLI
            if (
                len(sys.argv) >= 3
                and "airflow" in sys.argv[0]
                and sys.argv[1] == "users"
                and sys.argv[2] in USER_METHODS_TO_REMOVE
            ):
                raise AirflowException(
                    f"The 'airflow users {sys.argv[2]}' CLI command is disabled when Airflow RBAC configuration is enabled."
                )

    def sync_roles(self):
        super().sync_roles()

        # Add ALL_RBAC_ROLES_EXTRA_PERMISSIONS to all RBAC roles.
        all_roles = [role for role in self.get_all_roles()]
        for perm_tuple in ALL_RBAC_ROLES_EXTRA_PERMISSIONS:
            permission = self.create_permission(*perm_tuple)
            for role in all_roles:
                self.add_permission_to_role(role, permission)
        self.session.commit()

        # Remove user modification permissions when RBAC_BINDINGS are configured.
        # Environments that were created without RBAC_BINDINGS (or when bindings are enabled later)
        # have default User permissions that must be explicitly deleted.
        if RBAC_BINDINGS:
            self._remove_user_permissions()

    def add_permissions_view(self, base_permissions: list[str], view_menu: str) -> None:
        """
        Intercept permission creation to prevent modifications on Users.

        This prevents Airflow from recreating these permissions automatically on the Webserver startup.

        This is done when RBAC bindings are configured.
        """
        if RBAC_BINDINGS and view_menu == permissions.RESOURCE_USER:
            base_permissions = [p for p in base_permissions if p not in USER_PERMISSIONS_TO_REMOVE]
        super().add_permissions_view(base_permissions, view_menu)

    def update_user(self, user) -> bool:
        """
        Intercept user updates to prevent manual role modifications via the CLI.

        This is done when declarative RBAC bindings are configured.
        """
        self._check_cli_user_modifications()
        return super().update_user(user)

    def add_user(self, *args, **kwargs):
        """
        Intercept user creation to prevent manual creation via the CLI.

        This is done when declarative RBAC bindings are configured.
        """
        self._check_cli_user_modifications()
        return super().add_user(*args, **kwargs)

    @provide_session
    def find_user_by_username(self, username, session=None):
        """
        Find user by username.

        We use this custom method instead of find_user method of parent class, because parent class method
        sometimes may return stale User object.
        Note, that here we use session from provide_session decorator, not self.session - exactly in order to
        resolve the issue of returning stale object.
        """
        return session.scalars(
            select(self.user_model).where(func.lower(self.user_model.username) == func.lower(username))
        ).one_or_none()

    @retry(
        retry=retry_if_result(lambda user: user is None),  # Retry if the result is None.
        stop=stop_after_attempt(2),
        # Return result of the method instead of raising RetryError exception after two attempts.
        retry_error_callback=lambda retry_state: retry_state.outcome.result(),
    )
    def reconcile_user_roles(self, user, google_groups: list[str]):
        """
        Reconcile user roles based on RBAC_BINDINGS configuration.

        This takes into account user's email and its Google Groups memberships.
        """
        if not RBAC_BINDINGS:
            return user

        # When being called from get_or_register_user, the user is detached from the ORM session
        # since find_user_by_username uses short-living provide-session decorator which closes
        # the session after the method returns. We need to re-attach the user to the current session
        # before updating it, since we're touching user.roles field which triggers lazy-loading from DB (therefore requires session).
        if object_session(user) is not self.session:
            user = self.session.get(self.user_model, user.id)

        managed_identities = set()
        if user.email:
            managed_identities.add(f"user:{user.email.strip().lower()}")
        for g_name in google_groups:
            managed_identities.add(f"group:{g_name.strip().lower()}")

        expected_role_names = {
            binding.role for binding in RBAC_BINDINGS if managed_identities & set(binding.members)
        }
        current_role_names = {r.name for r in user.roles}

        if current_role_names != expected_role_names:
            log.info(
                "Reconciling roles for user %s. Current roles: %s. Expected roles: %s.",
                user.username,
                sorted(current_role_names),
                sorted(expected_role_names),
            )
            expected_roles = (
                self.session.scalars(
                    select(self.role_model).where(self.role_model.name.in_(expected_role_names))
                )
                .unique()
                .all()
            )

            user.roles = expected_roles
            update_result = self.update_user(user)
            if not update_result:
                # If update_user fails (e.g. DB conflict from a concurrent request),
                # returning None triggers a retry. By the 2nd attempt, the concurrent transaction
                # is expected to be committed, so the update should succeed.
                return None

        return user

    def load_user(self, user_id):
        user = super().load_user(user_id)
        if user:
            groups = self._get_groups_from_flask_request()
            if groups is not None:
                user = self.reconcile_user_roles(user, groups)
        return user
