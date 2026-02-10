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
from unittest import mock

import pytest
from fastapi import HTTPException, status

from airflow.api_fastapi.app import get_auth_manager
from airflow.composer.patches.rbac.composer_airflow_security_manager import ComposerAirflowSecurityManager
from airflow.providers.fab.auth_manager.api_fastapi.routes.login import login_router
from airflow.providers.fab.www.app import create_app

from tests_common.test_utils.config import conf_vars


class TestComposerAuthManager:
    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    def test_init(self):
        create_app(enable_plugins=False)
        am = get_auth_manager()
        assert "/token" in [r.path for r in login_router.routes]

        am.init()

        assert "/token" not in [r.path for r in login_router.routes]

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    def test_security_manager(self):
        create_app(enable_plugins=False)
        am = get_auth_manager()

        assert isinstance(am.security_manager, ComposerAirflowSecurityManager)

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch("airflow.composer.patches.rbac.composer_auth_manager.decode_inverting_proxy_jwt")
    @mock.patch("airflow.composer.patches.rbac.composer_auth_manager.get_or_register_user")
    def test_get_user_from_token(self, get_or_register_user_mock, decode_inverting_proxy_jwt_mock):
        create_app(enable_plugins=False)
        am = get_auth_manager()
        user_mock = mock.Mock()
        decode_inverting_proxy_jwt_mock.return_value = {
            "username": "test-username",
            "email": "test-email",
        }
        get_or_register_user_mock.return_value = user_mock

        actual_user = asyncio.run(am.get_user_from_token("test-token"))

        decode_inverting_proxy_jwt_mock.assert_called_once_with("test-token")
        get_or_register_user_mock.assert_called_once_with(username="test-username", email="test-email")
        assert actual_user == user_mock

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch("airflow.composer.patches.rbac.composer_auth_manager.decode_inverting_proxy_jwt")
    def test_get_user_from_token_decoding_error(self, decode_inverting_proxy_jwt_mock):
        create_app(enable_plugins=False)
        am = get_auth_manager()
        decode_inverting_proxy_jwt_mock.return_value = None

        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(am.get_user_from_token("test-token"))

        assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
        assert exc_info.value.detail == "Not authorized - unable to decode inverting proxy jwt"

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch("airflow.composer.patches.rbac.composer_auth_manager.decode_inverting_proxy_jwt")
    @mock.patch("airflow.composer.patches.rbac.composer_auth_manager.get_or_register_user")
    def test_get_user_from_token_not_registered(
        self, get_or_register_user_mock, decode_inverting_proxy_jwt_mock
    ):
        create_app(enable_plugins=False)
        am = get_auth_manager()
        decode_inverting_proxy_jwt_mock.return_value = {
            "username": "test-username",
            "email": "test-email",
        }
        get_or_register_user_mock.return_value = None

        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(am.get_user_from_token("test-token"))

        assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
        assert exc_info.value.detail == "Not authorized - unable to register or inactive user"

    @conf_vars(
        {("core", "auth_manager"): "airflow.composer.patches.rbac.composer_auth_manager.ComposerAuthManager"}
    )
    @mock.patch("airflow.composer.patches.rbac.composer_auth_manager.decode_inverting_proxy_jwt")
    @mock.patch("airflow.composer.patches.rbac.composer_auth_manager.get_or_register_user")
    def test_get_user_from_token_not_active(self, get_or_register_user_mock, decode_inverting_proxy_jwt_mock):
        create_app(enable_plugins=False)
        am = get_auth_manager()
        decode_inverting_proxy_jwt_mock.return_value = {
            "username": "test-username",
            "email": "test-email",
        }
        get_or_register_user_mock.return_value = mock.Mock(is_active=False)

        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(am.get_user_from_token("test-token"))

        assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
        assert exc_info.value.detail == "Not authorized - unable to register or inactive user"
