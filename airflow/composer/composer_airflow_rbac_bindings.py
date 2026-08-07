#
# Copyright 2026 Google LLC
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

import json
from dataclasses import dataclass

from airflow.configuration import conf
from airflow.security import permissions


@dataclass
class Binding:
    """Represents a single RBAC binding."""

    role: str
    members: list[str]


RBAC_BINDINGS = [Binding(**b) for b in json.loads(conf.get("api", "rbac_bindings", fallback="[]"))]

USER_PERMISSIONS_TO_REMOVE = [
    permissions.ACTION_CAN_CREATE,
    permissions.ACTION_CAN_EDIT,
]

USER_METHODS_TO_REMOVE = [
    "add-role",
    "remove-role",
    "create",
    "import",
]
