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

import random
import string
from unittest import mock

from airflow.composer.patches.rbac.monkey_patching.airflow_dag_processing_collection import patch
from airflow.dag_processing import collection
from airflow.providers.fab.www.security_appless import ApplessAirflowSecurityManager
from airflow.utils.session import provide_session


class TestAirflowDagProcessingCollection:
    def setup_method(self):
        patch()

    @provide_session
    @mock.patch(
        "airflow.composer.patches.rbac.monkey_patching.airflow_dag_processing_collection.RBAC_AUTOREGISTER_PER_FOLDER_ROLES",
        True,
    )
    def test_patch_sync_dag_perms(self, session):
        security_manager = ApplessAirflowSecurityManager(session=session)
        role_name = "".join(random.choice(string.ascii_lowercase) for _ in range(6))

        collection._sync_dag_perms(
            mock.Mock(
                dag_id="test-dag",
                access_control={role_name: {"DAGs": {"can_read"}}},
            ),
            session,
        )

        # Check that role is added.
        role = security_manager.find_role(role_name)
        assert role
        # Check that original function which syncs DAG permissions is called.
        assert len(role.permissions) == 1
        assert role.permissions[0].resource.name == "DAG:test-dag"
        assert role.permissions[0].action.name == "can_read"
