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

from unittest import mock

from airflow.composer.patches.rbac.airflow_local_settings import dag_policy


class TestAirflowLocalSettings:
    @mock.patch(
        "airflow.composer.patches.rbac.airflow_local_settings.RBAC_AUTOREGISTER_PER_FOLDER_ROLES",
        True,
    )
    @mock.patch("airflow.composer.patches.rbac.airflow_local_settings.apply_pfra_dag_policy", autospec=True)
    def test_dag_policy_pfra_enabled(self, apply_pfra_dag_policy_mock):
        dag_mock = mock.Mock()
        dag_policy(dag_mock)

        apply_pfra_dag_policy_mock.assert_called_once_with(dag_mock)

    @mock.patch(
        "airflow.composer.patches.rbac.airflow_local_settings.RBAC_AUTOREGISTER_PER_FOLDER_ROLES",
        False,
    )
    @mock.patch("airflow.composer.patches.rbac.airflow_local_settings.apply_pfra_dag_policy", autospec=True)
    def test_dag_policy_pfra_disabled(self, apply_pfra_dag_policy_mock):
        dag_mock = mock.Mock()
        dag_policy(dag_mock)

        apply_pfra_dag_policy_mock.assert_not_called()
