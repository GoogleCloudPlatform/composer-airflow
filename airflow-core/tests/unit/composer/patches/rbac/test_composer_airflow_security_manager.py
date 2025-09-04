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

from flask_appbuilder.const import AUTH_REMOTE_USER

from airflow.composer.patches.rbac.composer_airflow_security_manager import ComposerAirflowSecurityManager
from airflow.composer.patches.rbac.composer_auth_remote_user_view import ComposerAuthRemoteUserView
from airflow.providers.fab.www.app import create_app


class TestComposerAirflowSecurityManager:
    def test_composer_airflow_security_manager(self):
        app = create_app(enable_plugins=False)
        ComposerAirflowSecurityManager(app.appbuilder)

        assert ComposerAirflowSecurityManager.authremoteuserview == ComposerAuthRemoteUserView
        assert app.config["AUTH_TYPE"] == AUTH_REMOTE_USER
