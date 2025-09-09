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
import pem
from google.auth.transport.requests import AuthorizedSession
from tenacity import retry, retry_if_result, stop_after_attempt

from airflow.api_fastapi.app import get_auth_manager
from airflow.configuration import conf

JWT_PUBLIC_KEYS_URL = conf.get("webserver", "jwt_public_keys_url", fallback="")
INVERTING_PROXY_BACKEND_ID_REQUEST_HEADER = "X-Inverting-Proxy-Backend-ID"
INVERTING_PROXY_BACKEND_ID = conf.get("webserver", "inverting_proxy_backend_id", fallback="")
INVERTING_PROXY_USER_ID_REQUEST_HEADER = "X-Inverting-Proxy-User-ID"
RBAC_USER_REGISTRATION_ROLE = conf.get("webserver", "rbac_user_registration_role", fallback="")

log = logging.getLogger(__name__)

# Cached list of public keys to decode Inverting Proxy JWT.
JWT_PUBLIC_KEYS = None


def decode_inverting_proxy_jwt(inverting_proxy_jwt):
    """Retrieve and return username and email from decoded Inverting Proxy JWT."""
    global JWT_PUBLIC_KEYS

    if JWT_PUBLIC_KEYS is not None:
        # Try to decode with cached public keys. If decoding fails, then fallback to regular path with
        # fetching public keys from Inverting Proxy endpoint.
        log.debug("Decoding JWT with cached public keys")
        result = _decode_inverting_proxy_jwt_with_public_keys(inverting_proxy_jwt, JWT_PUBLIC_KEYS)
        if result is not None:
            return result

    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    authed_session = AuthorizedSession(credentials)

    log.debug("Fetching public keys for JWT verification")
    response = authed_session.request(
        "GET",
        JWT_PUBLIC_KEYS_URL,
        headers={INVERTING_PROXY_BACKEND_ID_REQUEST_HEADER: INVERTING_PROXY_BACKEND_ID},
    )
    if response.status_code != 200:
        log.error("Failed to fetch public keys for JWT verification, status: %s", response.status_code)
        return None

    JWT_PUBLIC_KEYS = [str(key) for key in pem.parse(response.text)]
    return _decode_inverting_proxy_jwt_with_public_keys(inverting_proxy_jwt, JWT_PUBLIC_KEYS)


# On the very first login of a user to Airflow UI, there might be a possible race condition when multiple
# API requests come in parallel. On every request this method:
# - checks if a user is already registered
# - if not, registers user
# If two requests check at the same time that user is not yet registered, then they will try both to
# register them and one of them will fail. Retrying this method, will make sure that both requests will be
# successful in case of such race condition.
@retry(
    retry=retry_if_result(lambda user: user is None),  # Retry if the result is None.
    stop=stop_after_attempt(2),  # Two attempts is enough to overcome mentioned above race condition.
    # Return result of the method instead of raising RetryError exception after two attempts.
    retry_error_callback=lambda retry_state: retry_state.outcome.result(),
)
def get_or_register_user(username, email):
    """Return user by given username and email. If user is not yet registered, register and return it."""
    appbuilder = get_auth_manager().appbuilder
    user = appbuilder.sm.find_user(username=username)

    if user is None:
        # Admin can preregister a user by setting user's email address as the username. When the
        # preregistered user opens Airflow UI for the first time, the email address is replaced with the
        # proper username (containing numerical identifier). This way the Google identity
        # (email address) is bound to the user account it represents at the time of user's first login.
        # See the following section about differences between Google identities and user accounts:
        # https://cloud.google.com/architecture/identity/overview-google-authentication#google_identities
        preregistered_user = appbuilder.sm.find_user(username=email)

        if preregistered_user:
            # User has been preregistered with email address as the username, update the record to set the
            # proper username.
            user = preregistered_user
            user.username = username
            update_result = appbuilder.sm.update_user(user)

            # We fail the login if we cannot update user record with the proper username.
            if not update_result:
                return None
        else:
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


def _decode_inverting_proxy_jwt_with_public_keys(inverting_proxy_jwt, public_keys):
    """
    Decode Inverting Proxy JWT with given public keys.

    Args:
        inverting_proxy_jwt: encoded JWT.
        public_keys: list of public key strings in the PEM file format.

    Returns:
      dictionary with decoded JWT payload or None if none of the given public keys couldn't successfully
      verify the JWT signature.
    """
    for ind, public_key in enumerate(public_keys):
        log.debug("Trying to decode jwt with public key, ind=%s", ind)
        try:
            decoded_jwt = jwt.decode(inverting_proxy_jwt, public_key, algorithms=["RS256"])
        except Exception:
            continue
        else:
            break
    else:
        log.error("JWT verification error with every public key")
        return None

    return {
        "username": decoded_jwt["sub"],
        "email": decoded_jwt["email"] if "email" in decoded_jwt else decoded_jwt["principal"],
    }
