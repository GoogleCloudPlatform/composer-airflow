# -*- coding: utf-8 -*-
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

import logging
import urllib.parse

from flask import g
from flask import get_flashed_messages
from flask import redirect
from flask import request
from flask_appbuilder import expose
from flask_appbuilder.security.views import AuthView
from flask_login import login_user
from flask_login import logout_user
from google.auth.transport import requests
from google.oauth2 import id_token

from airflow import configuration as conf
from airflow.www_rbac.security import AirflowSecurityManager


log = logging.getLogger(__file__)

# Expected audience of JWT.
AUDIENCE = conf.get("webserver", "google_oauth2_audience")


def _decode_iap_jwt(iap_jwt):
    """Returns username and email decoded from the given JWT.

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
            audience=AUDIENCE,
            certs_url="https://www.gstatic.com/iap/verify/public_key")
        return decoded_jwt["sub"], decoded_jwt["email"]
    except ValueError as e:
        log.error("JWT verification error: %s", e)
        return None, None


def _is_safe_redirect_url(next_url, host_url):
    """Checks if the URL is safe for redirects from this application.

    Args:
      next_url: Redirect URL to check.
      host_url: Host URL of this application.

    Returns:
      True if the checked URL is safe for redirects, False otherwise.
    """
    next_url_parsed = urllib.parse.urlparse(next_url)
    host_url_parsed = urllib.parse.urlparse(host_url)
    return next_url_parsed.scheme in ("http", "https") and \
           next_url_parsed.netloc == host_url_parsed.netloc


class ComposerAuthRemoteUserView(AuthView):
    """Authentication REMOTE_USER view for Composer."""
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

        iap_jwt = request.headers.get("X-Goog-IAP-JWT-Assertion")
        username, email = _decode_iap_jwt(iap_jwt)
        if username is None:
            return self.login_error_message, 403

        user = self._auth_remote_user(username=username, email=email)
        if user is None or not user.is_active:
            return self.login_error_message, 403

        # Flush any spurious "Access is Denied" flash message.
        get_flashed_messages()
        login_user(user)
        return self._redirect_back()

    def _auth_remote_user(self, username, email):
        """Fetches the specified user record or creates one if it doesn't exist.

        Also recognizes a user preregistered with email address as username, and
        updates their record to be identified with the proper username.

        Args:
          username: User's username for remote authentication.
          email: User's email to set in the user's record.

        Returns:
          The fetched or created user's record.
        """
        user = self.appbuilder.sm.find_user(username=username)
        if user is None:
            # Admin can preregister a user by setting user's email address as
            # the username. When the preregistered user opens Airflow UI for the
            # first time, the email address is replaced with the proper username
            # (containing numerical identifier). This way the Google identity
            # (email address) is bound to the user account it represents at the
            # time of user's first login. See the following section about
            # differences between Google identities and user accounts:
            # https://cloud.google.com/architecture/identity/overview-google-authentication#google_identities
            preregistered_user = self.appbuilder.sm.find_user(username=email)

            if preregistered_user:
                # User has been preregistered with email address as the
                # username, update the record to set the proper username.
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
                user = self.appbuilder.sm.add_user(
                    username=username,
                    first_name=email,
                    last_name="-",
                    email=email,
                    role=self.appbuilder.sm.find_role(
                        self.appbuilder.sm.auth_user_registration_role),
                )
                # Adding a user record can fail for example because of a
                # preregistered user with the same email but different
                # username.
                if not user:
                    return None

        self.appbuilder.sm.update_user_auth_stat(user)
        return user

    def _redirect_back(self):
        """Redirects to the originally requested URL."""
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


class ComposerAirflowSecurityManager(AirflowSecurityManager):
    """Airflow security manager adjusted for Composer."""
    authremoteuserview = ComposerAuthRemoteUserView

    # Hide User's Statistics page, which is broken due to a bug in
    # Flask-AppBuilder:
    # https://github.com/dpgaspar/Flask-AppBuilder/issues/1442.
    # The issue has already been fixed there but Airflow 1.10.* depends on an
    # older version of Flask-AppBuilder.
    userstatschartview = None

    def __init__(self, appbuilder):
        super(ComposerAirflowSecurityManager, self).__init__(appbuilder)
        if conf.getboolean(
            "webserver", "rbac_autoregister_per_folder_roles", fallback=False):
            # Add a role with permissions like in the User role except for
            # permissions to any DAGs. This role can be used as the user
            # registration role so that new users can open Airflow UI but
            # don't have access to any DAGs by default.
            self.ROLE_CONFIGS.append({
                "role": "NoDags",
                "perms": self.VIEWER_PERMS | self.USER_PERMS,
                "vms": self.VIEWER_VMS | self.USER_VMS,
            })
            # Note that the role hasn't been added to EXISTING_ROLES in
            # security.py. This means that AirflowSecurityManager will keep
            # synchronizing permissions from User role to NoDags role (
            # including per-DAG permissions, if added manually by admins to
            # User role, but excluding permissions on all_dags).
