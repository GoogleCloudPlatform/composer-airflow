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

from fastapi.security.oauth2 import OAuth2PasswordBearer

from airflow.composer.patches.rbac.monkey_patching.fastapi_security_oauth2 import patch


class TestFastapiSecurityOauth2:
    def setup_method(self):
        patch()

    def test_patch_oauth2_password_bearer_call(self):
        oauth2_flow = OAuth2PasswordBearer(tokenUrl="does-not-matter")

        actual_token = asyncio.run(
            oauth2_flow(
                request=mock.Mock(headers={"X-Inverting-Proxy-User-ID": "test-inverting-proxy-user-id"})
            )
        )

        assert actual_token == "test-inverting-proxy-user-id"
