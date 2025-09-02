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

import google.auth
import jwt
from google.auth.transport.requests import AuthorizedSession

from airflow.api_fastapi.app import get_auth_manager
from airflow.configuration import conf

JWT_PUBLIC_KEY_URL = conf.get("webserver", "jwt_public_key_url", fallback="")
INVERTING_PROXY_BACKEND_ID_REQUEST_HEADER = "X-Inverting-Proxy-Backend-ID"
INVERTING_PROXY_BACKEND_ID = conf.get("webserver", "inverting_proxy_backend_id", fallback="")
RBAC_USER_REGISTRATION_ROLE = conf.get("webserver", "rbac_user_registration_role", fallback="")

log = logging.getLogger(__name__)


def decode_inverting_proxy_jwt(inverting_proxy_jwt):
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


def get_or_register_user(username, email):
    """Return user. If user is not yet registered, register and return it."""
    appbuilder = get_auth_manager().appbuilder
    user = appbuilder.sm.find_user(username=username)

    if user is None:
        user = appbuilder.sm.add_user(
            username=username,
            first_name=email,
            last_name="-",
            email=email,
            role=appbuilder.sm.find_role(RBAC_USER_REGISTRATION_ROLE),
        )
        # Adding a user can fail for example because of another user with the same email but different
        # username.
        if not user:
            return None

        appbuilder.sm.update_user_auth_stat(user)

    return user
