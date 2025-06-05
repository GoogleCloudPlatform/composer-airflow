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

from airflow.composer.patches.core.configuration import ALL_RBAC_ROLES_EXTRA_PERMISSIONS
from airflow.composer.patches.webserver.composer_menu_plugin import COMPOSER_MENU_PLUGIN_PERMISSIONS
from airflow.composer.patches.webserver.initialize import initialize


class TestInitialize:
    def test_initialize(self):
        for perm in COMPOSER_MENU_PLUGIN_PERMISSIONS:
            if perm in ALL_RBAC_ROLES_EXTRA_PERMISSIONS:
                ALL_RBAC_ROLES_EXTRA_PERMISSIONS.remove(perm)
        assert not set(COMPOSER_MENU_PLUGIN_PERMISSIONS).issubset(ALL_RBAC_ROLES_EXTRA_PERMISSIONS)

        initialize()

        assert set(COMPOSER_MENU_PLUGIN_PERMISSIONS).issubset(ALL_RBAC_ROLES_EXTRA_PERMISSIONS)
