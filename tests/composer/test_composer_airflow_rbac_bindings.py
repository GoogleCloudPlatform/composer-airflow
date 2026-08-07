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

import importlib

from airflow.composer import composer_airflow_rbac_bindings
from tests.test_utils.config import conf_vars


class TestComposerAirflowRbacBindings:
    def test_rbac_bindings(self):
        json = (
            '[{"role": "Admin", "members": ["user:test-user@example.com"]}, '
            '{"role": "Viewer", "members": ["group:test-group@example.com"]}]'
        )

        with conf_vars({("api", "rbac_bindings"): json}):
            importlib.reload(composer_airflow_rbac_bindings)
            bindings = composer_airflow_rbac_bindings.RBAC_BINDINGS

        assert len(bindings) == 2
        assert bindings[0].role == "Admin"
        assert bindings[0].members == ["user:test-user@example.com"]
        assert bindings[1].role == "Viewer"
        assert bindings[1].members == ["group:test-group@example.com"]

    def test_rbac_bindings_fallback(self):
        with conf_vars({("api", "rbac_bindings"): "[]"}):
            importlib.reload(composer_airflow_rbac_bindings)
            bindings = composer_airflow_rbac_bindings.RBAC_BINDINGS

        assert bindings == []
