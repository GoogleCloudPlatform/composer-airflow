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
from fastapi.security.oauth2 import OAuth2PasswordBearer

from airflow.composer.patches.rbac.monkey_patching.fastapi_security_oauth2 import patch


class TestFastapiSecurityOauth2:
    def setup_method(self):
        patch()

    def test_patch_session_id_from_cookie(self):
        oauth2_flow = OAuth2PasswordBearer(tokenUrl="does-not-matter")

        actual_token = asyncio.run(oauth2_flow(request=mock.Mock(cookies={"session": "test-session-id"})))

        assert actual_token == "test-session-id"

    def test_patch_session_id_from_header(self):
        oauth2_flow = OAuth2PasswordBearer(tokenUrl="does-not-matter")

        actual_token = asyncio.run(
            oauth2_flow(request=mock.Mock(cookies={}, headers={"Auth-Token": "test-session-id2"}))
        )

        assert actual_token == "test-session-id2"

    def test_patch_no_session_id(self):
        oauth2_flow = OAuth2PasswordBearer(tokenUrl="does-not-matter")

        with pytest.raises(HTTPException) as e:
            asyncio.run(oauth2_flow(request=mock.Mock(cookies={}, headers={})))

        assert e.value.status_code == status.HTTP_401_UNAUTHORIZED
        assert e.value.detail == "Not authenticated - no session cookie"
