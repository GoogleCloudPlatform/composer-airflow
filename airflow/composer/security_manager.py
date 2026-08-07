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
"""Airflow Composer security manager implementation."""

from __future__ import annotations

import logging
import sys
import urllib.parse
from typing import Collection

import jwt
import pem
from flask import g, get_flashed_messages, redirect, request
from flask_appbuilder import expose
from flask_appbuilder.security.views import AuthView
from flask_login import login_user, logout_user
from google import auth  # type: ignore
from google.auth.transport import requests
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import id_token
from jwt.exceptions import InvalidSignatureError
from sqlalchemy import select
from sqlalchemy.orm import object_session
from tenacity import retry, retry_if_result, stop_after_attempt

from airflow.composer.composer_airflow_rbac_bindings import (
    RBAC_BINDINGS,
    USER_METHODS_TO_REMOVE,
    USER_PERMISSIONS_TO_REMOVE,
)
from airflow.composer.plugins.composer_menu_plugin import COMPOSER_MENU_PLUGIN_PERMISSIONS
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import DagBag
from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride
from airflow.security import permissions
from airflow.www.security import EXISTING_ROLES

log = logging.getLogger(__name__)


def _decode_iap_jwt(iap_jwt):
    """
    Return username and email decoded from the given IAP JWT.

    Args:
      iap_jwt: JWT from Cloud IAP.

    Returns:
      Decoded username and email.
    """
    try:
        # Token verification and user identity retrieval as described in
        # https://cloud.google.com/iap/docs/signed-headers-howto#retrieving_the_user_identity
        decoded_jwt = id_token.verify_token(
            iap_jwt,
            requests.Request(),
            audience=conf.get("webserver", "google_oauth2_audience"),
            certs_url="https://www.gstatic.com/iap/verify/public_key",
        )
        return decoded_jwt["sub"], decoded_jwt["email"]
    except ValueError as e:
        log.error("JWT verification error: %s", e)
        return None, None


def _decode_inverting_proxy_jwt(inverting_proxy_jwt):
    """
    Decode the given Inverting Proxy JWT.

    Return username, email (or IAM principal for BYOID users),
       display_username, and google_groups decoded from the given Inverting Proxy JWT.

    Args:
      inverting_proxy_jwt: JWT from Inverting Proxy.

    Returns:
      Decoded username, email (or IAM principal), display_username, and google_groups.
    """
    try:
        credentials, _ = auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        authed_session = AuthorizedSession(credentials)
        headers = {"X-Inverting-Proxy-Backend-ID": conf.get("webserver", "inverting_proxy_backend_id")}
        response = authed_session.request(
            "GET", conf.get("webserver", "jwt_public_keys_url"), headers=headers
        )
        if response.status_code != 200:
            log.error("Failed to fetch public key for JWT verification, status: %s", response.status_code)
            return None, None, None, []
        # The response may contain multiple concatenated public keys. The verification is successful
        # if one of the keys can verify the signature.
        public_keys = [str(key) for key in pem.parse(response.text)]
        decoded_jwt = _try_decoding_jwt_with_keys(inverting_proxy_jwt, public_keys)
        email_or_principal = decoded_jwt["email"] if "email" in decoded_jwt else decoded_jwt["principal"]
        # display_username is available only for BYOID users.
        return (
            decoded_jwt["sub"],
            email_or_principal,
            decoded_jwt.get("display_username"),
            decoded_jwt.get("groups", []),
        )
    except Exception as e:  # pylint: disable=broad-except
        log.error("JWT verification error: %s", e)
        return None, None, None, []


def _try_decoding_jwt_with_keys(encoded_jwt, public_keys):
    """
    Try to decode the given JWT with each of the given public keys.

    Args:
      encoded_jwt: Encoded JWT.
      public_keys: List of public key strings in the PEM file format.

    Returns:
      Dict with decoded JWT payload.

    Raises:
      InvalidSignatureError: If none of the given keys successfully verifies the JWT signature.
    """
    for public_key in public_keys:
        try:
            return jwt.decode(encoded_jwt, public_key, algorithms=["RS256"])
        except InvalidSignatureError:
            continue
    raise InvalidSignatureError("JWT signature matches none of the public keys")


def _get_first_and_last_name(display_username, email_or_principal):
    """
    Return the first_name and last_name for a user.

    Args:
      display_username: for BYOID users, display_username is of the form:
      'Subject (Workforce Pool name)'. None otherwise.
      email_or_principal: email for first party users, or IAM principal
      for BYOID users.

    Returns:
      subject as first_name and workforce pool name as last_name for
      BYOID users; email as first_name and `-` as last_name for
      first party users.
    """
    if not display_username:
        return email_or_principal, "-"
    idx = display_username.rfind(" (")
    if idx == -1:
        # Unexpected value in display_username, return it as is for
        # first_name and "-" for last_name.
        return display_username, "-"
    return display_username[:idx], display_username[idx + 1 :]


def _is_safe_redirect_url(next_url, host_url):
    """
    Check if the URL is safe for redirects from this application.

    Args:
      next_url: Redirect URL to check.
      host_url: Host URL of this application.

    Returns:
      True if the checked URL is safe for redirects, False otherwise.
    """
    next_url_parsed = urllib.parse.urlparse(next_url)
    host_url_parsed = urllib.parse.urlparse(host_url)
    return next_url_parsed.scheme in ("http", "https") and next_url_parsed.netloc == host_url_parsed.netloc


class ComposerAuthRemoteUserView(AuthView):
    """Act as Authentication REMOTE_USER view for Composer."""

    login_template = ""
    login_error_message = "Not authorized or account inactive"

    @expose("/login/")
    def login(self):
        if g.user is not None and g.user.is_authenticated:
            # This request is most likely coming from access control handler,
            # which redirects to login URL when the currently logged in user
            # doesn't have access to the originally requested page. We need to
            # ignore the 'next' parameter in this case, to avoid infinite
            # redirect loop. Instead, we redirect to the homepage, which should
            # show 'Access is Denied' message.
            return redirect(self.appbuilder.get_url_for_index)

        # Authenticate user from current request, return 403 in case of not
        # valid credentials.
        if self.auth_current_user() is None:
            return self.login_error_message, 403

        # Flush any spurious "Access is Denied" flash message.
        get_flashed_messages()
        return self._redirect_back()

    def auth_current_user(self, user_registration_role=None):
        """Authenticate and set current user if appropriate header exists."""
        if "X-Goog-IAP-JWT-Assertion" in request.headers:
            iap_jwt = request.headers.get("X-Goog-IAP-JWT-Assertion")
            username, email = _decode_iap_jwt(iap_jwt)
            display_username = None
            google_groups = []
        elif "X-Inverting-Proxy-User-ID" in request.headers:
            inverting_proxy_jwt = request.headers.get("X-Inverting-Proxy-User-ID")
            username, email, display_username, google_groups = _decode_inverting_proxy_jwt(
                inverting_proxy_jwt
            )
        else:
            return None

        if username is None:
            return None

        user = self._auth_remote_user(
            username=username,
            email=email,
            display_username=display_username,
            google_groups=google_groups,
            user_registration_role=user_registration_role,
        )
        if user is None or not user.is_active:
            return None

        login_user(user)
        return user

    def _auth_remote_user(
        self, username, email, display_username, google_groups, user_registration_role=None
    ):
        """
        Fetch the specified user record or creates one if it doesn't exist.

        Also recognize a user preregistered with email address or IAM principal
        as username, and updates their record to be identified with the proper
        username.

        Args:
          username: User's username for remote authentication.
          email: User's email, or BYOID user's IAM principal, to set in the
            user's record.
          display_username: User's display username from which the first_name
            and last_name will be derived before setting them in the user's
            record.
          google_groups: List of user's Google Groups from JWT.
          user_registration_role: User's role in case it will be registered
            (created). If not passed, AUTH_USER_REGISTRATION_ROLE from
            webserver_config.py will be used.

        Returns:
          The fetched or created user's record.
        """
        user = self.appbuilder.sm.find_user(username=username)
        if user is None:
            # Admin can preregister a user by setting user's email address, or
            # BYOID user's IAM principal, as the username. When the
            # preregistered user opens Airflow UI for the first time, the email
            # address or principal is replaced with the proper username
            # (containing numerical identifier). This way the Google identity
            # (email address) or federated workforce identity (subject's IAM
            # principal) is bound to the user account it represents at the time
            # of user's first login. See the following section about differences
            # between Google identities and user accounts:
            # https://cloud.google.com/architecture/identity/overview-google-authentication#google_identities
            # See the following section about workforce identity federation:
            # https://cloud.google.com/iam/docs/workforce-identity-federation#what_is_workforce_identity_federation
            preregistered_user = self.appbuilder.sm.find_user(username=email)

            if preregistered_user:
                # User has been preregistered with email address or IAM
                # principal as the username, update the record to set the
                # proper username.
                user = preregistered_user
                user.username = username
                update_result = self.appbuilder.sm.update_user(user)
                # We fail the login if we cannot update user record with the
                # proper username in the user record. Note that update_user
                # returns any value (False) only in case of an error so we
                # compare with False explicitly to avoid entering the block
                # when update_result is None.
                if update_result is False:
                    return None
            else:
                # User does not exist and has not been preregistered, create
                # one.
                first_name, last_name = _get_first_and_last_name(display_username, email)
                user = self.appbuilder.sm.add_user(
                    username=username,
                    first_name=first_name,
                    last_name=last_name,
                    email=email,
                    role=self.appbuilder.sm.find_role(
                        user_registration_role or self.appbuilder.sm.auth_user_registration_role
                    ),
                )
                # Adding a user record can fail for example because of a
                # preregistered user with the same email but different
                # username.
                if not user:
                    return None

        self.appbuilder.sm.update_user_auth_stat(user)
        user = self.appbuilder.sm.reconcile_user_roles(user, google_groups)
        return user

    def _redirect_back(self):
        """Redirect to the originally requested URL."""
        next_url = request.args.get("next")
        host_url = request.host_url

        # The URL retrieved from 'next' parameter must be validated as
        # documented in
        # https://flask-login.readthedocs.io/en/latest/#login-example
        if next_url and _is_safe_redirect_url(next_url, host_url):
            return redirect(next_url)

        # Fallback to index URL.
        return redirect(self.appbuilder.get_url_for_index)

    @expose("/logout/")
    def logout(self):
        logout_user()
        # The /logout path isn't linked from Airflow RBAC UI in Composer
        # because of no suitable implementation under Cloud IAP. But if the
        # user visits this path anyway, we log them out of their Google
        # Account.
        return redirect("https://accounts.google.com/logout")


class ComposerAirflowSecurityManager(FabAirflowSecurityManagerOverride):
    """Airflow security manager adjusted for Composer."""

    authremoteuserview = ComposerAuthRemoteUserView

    def __init__(self, appbuilder):
        super().__init__(appbuilder)
        if conf.getboolean("webserver", "rbac_autoregister_per_folder_roles", fallback=False):
            # Add a role with permissions like in the User role except for
            # permissions to any DAGs. This role can be used as the user
            # registration role so that new users can open Airflow UI but
            # don't have access to any DAGs by default.
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

        # Add access to all Composer Menu items for all existing roles
        for role in EXISTING_ROLES:
            self.ROLE_CONFIGS.append(
                {
                    "role": role,
                    "perms": COMPOSER_MENU_PLUGIN_PERMISSIONS,
                }
            )

    @staticmethod
    def _get_groups_from_flask_request() -> list[str] | None:
        """
        Extract user groups from the JWT token in request headers from flask request.

        Returns:
            A list of group names if groups are present in the valid JWT token.
            None if unable to extract groups from the token, the token is missing, or decoding failed.
        """
        jwt_token = request.headers.get("X-Inverting-Proxy-User-ID")
        if not jwt_token:
            return None

        username, _, _, google_groups = _decode_inverting_proxy_jwt(jwt_token)
        if username is None:
            return None
        return google_groups

    def _remove_user_permissions(self) -> None:
        """
        Remove ability to manually edit Users.

        If the declarative RBAC config is disabled, these permissions will be automatically recreated
        and assigned to appropriate default roles by Airflow itself during its regular RBAC sync.
        """
        perms = self.get_session.scalars(
            select(self.permission_model)
            .join(self.action_model)
            .join(self.resource_model)
            .where(
                self.action_model.name.in_(USER_PERMISSIONS_TO_REMOVE),
                self.resource_model.name == permissions.RESOURCE_USER,
            )
        ).all()
        for perm in perms:
            for role in list(perm.role):
                role.permissions.remove(perm)
            self.get_session.delete(perm)

        self.get_session.commit()

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
        self.add_composer_menu_access_to_custom_roles()

        # Remove user modification permissions when RBAC_BINDINGS are configured.
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

    @retry(
        retry=retry_if_result(lambda user: user is None),
        stop=stop_after_attempt(2),
        retry_error_callback=lambda retry_state: retry_state.outcome.result(),
    )
    def reconcile_user_roles(self, user, google_groups: list[str]):
        """
        Reconcile user roles based on RBAC_BINDINGS configuration.

        This takes into account user's email and its Google Groups memberships.
        """
        if not RBAC_BINDINGS:
            return user

        session = self.get_session
        if object_session(user) is not session:
            db_user = session.query(self.user_model).get(user.id)
            if db_user:
                user = db_user

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
                session.query(self.role_model).filter(self.role_model.name.in_(expected_role_names)).all()
            )

            user.roles = expected_roles
            update_result = self.update_user(user)
            if not update_result:
                return None

        return user

    def load_user(self, user_id):
        user = super().load_user(user_id)
        if user:
            groups = self._get_groups_from_flask_request()
            if groups is not None:
                user = self.reconcile_user_roles(user, groups)
        return user

    def add_composer_menu_access_to_custom_roles(self):
        """Add access to Composer Menu items for all custom roles."""
        custom_roles = [role for role in self.get_all_roles() if role.name not in EXISTING_ROLES]
        for role in custom_roles:
            for permission in COMPOSER_MENU_PLUGIN_PERMISSIONS:
                self.add_permission_to_role(role, self.create_permission(*permission))
        self.get_session.commit()

    def create_dag_specific_permissions(self):
        from airflow.configuration import conf

        super().create_dag_specific_permissions()

        if conf.getboolean("webserver", "rbac_autoregister_per_folder_roles", fallback=False):
            dagbag = DagBag(read_dags_from_db=True)
            dagbag.collect_dags_from_db()
            dags = dagbag.dags.values()
            for dag in dags:
                root_dag_id = dag.parent_dag.dag_id if dag.parent_dag else dag.dag_id
                dag_resource_name = permissions.resource_name_for_dag(root_dag_id)
                self.sync_perm_for_dag(dag_resource_name, dag.access_control or {})

    def sync_perm_for_dag(
        self,
        dag_id: str,
        access_control: dict[str, dict[str, Collection[str]]] | None = None,
    ) -> None:
        from airflow.configuration import conf

        super().sync_perm_for_dag(dag_id=dag_id, access_control=access_control)

        if conf.getboolean("webserver", "rbac_autoregister_per_folder_roles", fallback=False):
            dag_resource_name = permissions.resource_name_for_dag(dag_id)
            self.log.debug("Syncing DAG-level permissions for DAG '%s'", dag_resource_name)
            self._sync_dag_view_permissions(dag_resource_name, access_control or {})
