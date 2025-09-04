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

from functools import cached_property

from fastapi import HTTPException, status

from airflow.composer.patches.rbac.composer_airflow_security_manager import ComposerAirflowSecurityManager
from airflow.composer.patches.rbac.utils import (
    decode_inverting_proxy_jwt,
    get_or_register_user,
)
from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager


class ComposerAuthManager(FabAuthManager):
    """FAB Auth Manager adjusted per Composer needs."""

    @cached_property
    def security_manager(self):
        return ComposerAirflowSecurityManager(self.appbuilder)

    async def get_user_from_token(self, token):
        """Retrieve and return a user by given token."""
        # `token` is Inverting Proxy JWT.
        decoded_inverting_proxy_jwt = decode_inverting_proxy_jwt(token)
        if not decoded_inverting_proxy_jwt:
            raise HTTPException(
                status.HTTP_401_UNAUTHORIZED, "Not authorized - unable to decode inverting proxy jwt"
            )

        username = decoded_inverting_proxy_jwt["username"]
        email = decoded_inverting_proxy_jwt["email"]
        user = get_or_register_user(
            username=username,
            email=email,
        )

        if user is None or not user.is_active:
            raise HTTPException(
                status.HTTP_401_UNAUTHORIZED, "Not authorized - unable to register or inactive user"
            )

        return user
