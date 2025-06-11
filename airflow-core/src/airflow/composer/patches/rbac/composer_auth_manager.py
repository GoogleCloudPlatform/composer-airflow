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
from flask_session.sessions import want_bytes

from airflow.composer.patches.rbac.composer_airflow_security_manager import ComposerAirflowSecurityManager
from airflow.providers.fab.auth_manager.api_fastapi.routes.login import login_router
from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager
from airflow.providers.fab.auth_manager.models import User
from airflow.utils.session import create_session as create_sqla_session


class ComposerAuthManager(FabAuthManager):
    """FAB Auth Manager adjusted per Composer needs."""

    def init(self):
        # Remove FAB route for "/token" path. Composer route for this path will be registered in
        # ComposerAuthRemoteUserView.
        login_router.routes = [r for r in login_router.routes if r.path != "/token"]

        return super().init()

    @cached_property
    def security_manager(self):
        return ComposerAirflowSecurityManager(self.appbuilder)

    async def get_user_from_token(self, token):
        """Retrieve and return a user by given token."""
        flask_app = self.appbuilder.app
        session_interface = flask_app.session_interface
        session_model = session_interface.sql_session_model

        # token has following format: {session_id}.{tail}
        session_id = token.split(".")[0]

        with create_sqla_session() as sqla_session:
            session = sqla_session.query(session_model).filter(session_model.session_id == session_id).first()
            if not session:
                # Flask session is not found, most likely expired and removed.
                raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Not authorized - session expired")

            session_data = session_interface.serializer.loads(want_bytes(session.data))
            user_id = session_data.get("_user_id")
            user = sqla_session.get(User, user_id)
            if not user:
                # User was removed from Airflow database.
                raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Not authorized - user not found")

            return user
