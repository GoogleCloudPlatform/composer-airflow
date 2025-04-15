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

from airflow.composer.patches.rbac.composer_auth_remote_user_view import ComposerAuthRemoteUserView
from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride


class ComposerAirflowSecurityManager(FabAirflowSecurityManagerOverride):
    """FAB security manager adjusted per Composer needs."""

    authremoteuserview = ComposerAuthRemoteUserView

    def _init_config(self):
        app = self.appbuilder.get_app
        app.config["AUTH_TYPE"] = AUTH_REMOTE_USER

        super()._init_config()
