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
import urllib.parse

import google.auth
import jwt
from flask import get_flashed_messages, redirect, request
from flask_appbuilder import expose
from flask_appbuilder.security.views import AuthRemoteUserView
from flask_login import login_user
from google.auth.transport.requests import AuthorizedSession

from airflow.configuration import conf

INVERTING_PROXY_USER_ID_REQUEST_HEADER = "X-Inverting-Proxy-User-ID"
INVERTING_PROXY_BACKEND_ID_REQUEST_HEADER = "X-Inverting-Proxy-Backend-ID"
INVERTING_PROXY_BACKEND_ID = conf.get("webserver", "inverting_proxy_backend_id", fallback="")
JWT_PUBLIC_KEY_URL = conf.get("webserver", "jwt_public_key_url", fallback="")
RBAC_USER_REGISTRATION_ROLE = conf.get("webserver", "rbac_user_registration_role", fallback="")

log = logging.getLogger(__name__)


class ComposerAuthRemoteUserView(AuthRemoteUserView):
    """Auth Remote User View adjusted per Composer needs."""

    @expose("/login/")
    def login(self):
        # Authenticate user, return 403 in case of failure.
        if self.auth_current_user() is None:
            return "Not authorized or account inactive", 403

        # Flush any spurious "Access is Denied" flash message.
        get_flashed_messages()
        return self._redirect_back()

    @expose("/logout/")
    def logout(self):
        response = super().logout()
        # Delete DATALAB_TUNNEL_TOKEN cookie to force user visit page with Google account selection.
        response.delete_cookie("DATALAB_TUNNEL_TOKEN")

        return response

    def auth_current_user(self):
        """Authenticate user by using appropriate header in request."""
        if INVERTING_PROXY_USER_ID_REQUEST_HEADER not in request.headers:
            return None

        inverting_proxy_jwt = request.headers.get(INVERTING_PROXY_USER_ID_REQUEST_HEADER)
        decoded_inverting_proxy_jwt = self._decode_inverting_proxy_jwt(inverting_proxy_jwt)
        if not decoded_inverting_proxy_jwt:
            return None

        username = decoded_inverting_proxy_jwt["username"]
        email = decoded_inverting_proxy_jwt["email"]

        user = self._register_user_if_needed(
            username=username,
            email=email,
        )
        if user is None or not user.is_active:
            return None

        login_user(user)
        return user

    def _decode_inverting_proxy_jwt(self, inverting_proxy_jwt):
        """Retrieve and return username and email from decoded Inverting Proxy JWT."""
        try:
            credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
            authed_session = AuthorizedSession(credentials)

            response = authed_session.request(
                "GET",
                JWT_PUBLIC_KEY_URL,
                headers={INVERTING_PROXY_BACKEND_ID_REQUEST_HEADER: INVERTING_PROXY_BACKEND_ID},
            )
            if response.status_code != 200:
                log.error("Failed to fetch public key for JWT verification, status: %s", response.status_code)
                return None

            public_key = response.text
            decoded_jwt = jwt.decode(inverting_proxy_jwt, public_key, algorithms=["RS256"])

            return {
                "username": decoded_jwt["sub"],
                "email": decoded_jwt["email"] if "email" in decoded_jwt else decoded_jwt["principal"],
            }
        except Exception as e:
            log.error("JWT verification error: %s", e)
            return None

    def _register_user_if_needed(self, username, email):
        """Register user if not yet registered, and return it."""
        user = self.appbuilder.sm.find_user(username=username)

        if user is None:
            # Admin can preregister a user by setting user's email address as the username. When the
            # preregistered user opens Airflow UI for the first time, the email address is replaced with the
            # proper username (containing numerical identifier). This way the Google identity
            # (email address) is bound to the user account it represents at the time of user's first login.
            # See the following section about differences between Google identities and user accounts:
            # https://cloud.google.com/architecture/identity/overview-google-authentication#google_identities
            preregistered_user = self.appbuilder.sm.find_user(username=email)

            if preregistered_user:
                # User has been preregistered with email address as the username, update the record to set the
                # proper username.
                user = preregistered_user
                user.username = username
                update_result = self.appbuilder.sm.update_user(user)

                # We fail the login if we cannot update user record with the proper username.
                if not update_result:
                    return None
            else:
                user = self.appbuilder.sm.add_user(
                    username=username,
                    first_name=email,
                    last_name="-",
                    email=email,
                    role=self.appbuilder.sm.find_role(RBAC_USER_REGISTRATION_ROLE),
                )
                # Adding a user can fail for example because of another user with the same email but different
                # username.
                if not user:
                    return None

        self.appbuilder.sm.update_user_auth_stat(user)
        return user

    def _redirect_back(self):
        """Redirect to the originally requested URL."""
        next_url = request.args.get("next")
        host_url = request.host_url

        # The URL retrieved from 'next' parameter must be validated as documented in
        # https://flask-login.readthedocs.io/en/latest/#login-example
        if next_url and self._is_safe_redirect_url(next_url, host_url):
            return redirect(next_url)

        return redirect(self.appbuilder.get_url_for_index)

    def _is_safe_redirect_url(self, next_url, host_url):
        """Check if the URL is safe for redirects from this application."""
        next_url_parsed = urllib.parse.urlparse(next_url)
        host_url_parsed = urllib.parse.urlparse(host_url)

        return (
            next_url_parsed.scheme in ("http", "https") and next_url_parsed.netloc == host_url_parsed.netloc
        )
