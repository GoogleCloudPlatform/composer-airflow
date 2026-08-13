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

import functools
from typing import TYPE_CHECKING

from fastapi.security.oauth2 import OAuth2PasswordBearer

from airflow.composer.patches.rbac.utils import INVERTING_PROXY_USER_ID_REQUEST_HEADER

if TYPE_CHECKING:
    from fastapi import Request


def patch():
    # Patch FastAPI OAuth2 flow for authentication to use Inverting Proxy user ID header as an auth token
    # instead of Authorization header.
    OAuth2PasswordBearer.__call__ = _composer_oauth2_password_bearer_call(OAuth2PasswordBearer.__call__)


def _composer_oauth2_password_bearer_call(f):
    @functools.wraps(f)
    async def wrapper(self, request: Request):
        # Note, we do not call "f" (original method) here and this is intended as we completely replace (not
        # extend) previous implementation/logic.
        return request.headers.get(INVERTING_PROXY_USER_ID_REQUEST_HEADER)

    return wrapper
