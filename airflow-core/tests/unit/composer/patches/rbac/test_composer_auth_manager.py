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

import asyncio
import random
import string

import pytest
from fastapi import HTTPException, status

from airflow.composer.patches.rbac.composer_airflow_security_manager import ComposerAirflowSecurityManager
from airflow.composer.patches.rbac.composer_auth_manager import ComposerAuthManager
from airflow.providers.fab.auth_manager.models import User
from airflow.providers.fab.www.app import create_app
from airflow.utils.session import create_session as create_sqla_session


class TestComposerAuthManager:
    def setup_method(self):
        self.app = create_app(enable_plugins=False)
        self.am = ComposerAuthManager()
        self.am.appbuilder = self.app.appbuilder

    def test_security_manager(self):
        assert isinstance(self.am.security_manager, ComposerAirflowSecurityManager)

    def test_get_user_from_token(self):
        user_id = random.randint(1000, 100000)
        session_interface = self.app.session_interface
        session_model = session_interface.sql_session_model
        session_id = "".join(random.choice(string.ascii_uppercase) for _ in range(6))

        with create_sqla_session() as sqla_session:
            sqla_session.add(
                User(
                    id=user_id,
                    first_name="f",
                    last_name="l",
                    username=f"u_{session_id}",
                    email=f"e_{session_id}",
                )
            )
            sqla_session.add(
                session_model(
                    session_id=session_id,
                    data=session_interface.serializer.dumps(
                        {
                            "_user_id": user_id,
                        }
                    ),
                    expiry=None,
                )
            )
            sqla_session.commit()

        actual_user = asyncio.run(self.am.get_user_from_token(f"{session_id}.tail"))

        assert isinstance(actual_user, User)
        assert actual_user.id == user_id
        assert actual_user.username == f"u_{session_id}"

    def test_get_user_from_token_session_expired(self):
        with pytest.raises(HTTPException) as e:
            asyncio.run(self.am.get_user_from_token("not_exist.tail"))

        assert e.value.status_code == status.HTTP_401_UNAUTHORIZED
        assert e.value.detail == "Not authorized - session expired"

    def test_get_user_from_token_user_not_found(self):
        session_interface = self.app.session_interface
        session_model = session_interface.sql_session_model
        session_id = "".join(random.choice(string.ascii_uppercase) for _ in range(6))

        with create_sqla_session() as sqla_session:
            sqla_session.add(
                session_model(
                    session_id=session_id,
                    data=session_interface.serializer.dumps(
                        {
                            "_user_id": 1579987,  # not existing
                        }
                    ),
                    expiry=None,
                )
            )
            sqla_session.commit()

        with pytest.raises(HTTPException) as e:
            asyncio.run(self.am.get_user_from_token(f"{session_id}.tail"))

        assert e.value.status_code == status.HTTP_401_UNAUTHORIZED
        assert e.value.detail == "Not authorized - user not found"
