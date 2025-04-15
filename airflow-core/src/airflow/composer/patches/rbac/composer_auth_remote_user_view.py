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

import urllib.parse

from flask import (
    get_flashed_messages,
    redirect,
    request,
)
from flask_appbuilder import expose
from flask_appbuilder.security.views import AuthRemoteUserView
from flask_login import login_user

from airflow.composer.patches.rbac.utils import (
    INVERTING_PROXY_USER_ID_REQUEST_HEADER,
    decode_inverting_proxy_jwt,
    get_or_register_user,
)


class ComposerAuthRemoteUserView(AuthRemoteUserView):
    """Auth Remote User View adjusted per Composer needs."""

    @expose("/login/")
    def login(self):
        inverting_proxy_jwt = request.headers.get(INVERTING_PROXY_USER_ID_REQUEST_HEADER)

        decoded_inverting_proxy_jwt = decode_inverting_proxy_jwt(inverting_proxy_jwt)
        if not decoded_inverting_proxy_jwt:
            return "Not authorized - unable to decode inverting proxy JWT", 401

        username = decoded_inverting_proxy_jwt["username"]
        email = decoded_inverting_proxy_jwt["email"]
        user = get_or_register_user(
            username=username,
            email=email,
        )

        if user is None or not user.is_active:
            return "Not authorized - unable to register or inactive user", 401

        login_user(user)

        # Flush any spurious "Access is Denied" flash message.
        get_flashed_messages()
        return self._redirect_back()

    @expose("/logout/")
    def logout(self):
        response = super().logout()
        # Delete DATALAB_TUNNEL_TOKEN cookie to force user visit page with Google account selection.
        response.delete_cookie("DATALAB_TUNNEL_TOKEN")

        return response

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
