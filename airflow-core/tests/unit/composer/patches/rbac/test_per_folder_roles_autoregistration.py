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

import os
from unittest import mock

from airflow import settings
from airflow.composer.patches.rbac.per_folder_roles_autoregistration import apply_pfra_dag_policy


class TestPerFolderRolesAutoregistration:
    def test_apply_pfra_dag_policy_access_control_empty(self):
        dag_mock = mock.Mock(
            fileloc=os.path.join(settings.DAGS_FOLDER, "role_1/dag_1.py"),
            access_control=None,
        )

        apply_pfra_dag_policy(dag_mock)

        assert dag_mock.access_control == {
            "role_1": {
                "DAGs": {"can_edit", "can_read"},
            },
        }

    def test_apply_pfra_dag_policy_access_control_not_empty(self):
        dag_mock = mock.Mock(
            fileloc=os.path.join(settings.DAGS_FOLDER, "role_1/dag_1.py"),
            access_control={
                "role_2": {
                    "DAGs": {"can_create"},
                },
            },
        )

        apply_pfra_dag_policy(dag_mock)

        assert dag_mock.access_control == {
            "role_1": {
                "DAGs": {"can_edit", "can_read"},
            },
            "role_2": {
                "DAGs": {"can_create"},
            },
        }

    def test_apply_pfra_dag_policy_access_control_already_has_this_role(self):
        dag_mock = mock.Mock(
            fileloc=os.path.join(settings.DAGS_FOLDER, "role_1/dag_1.py"),
            access_control={
                "role_1": {
                    "DAG Runs": {"can_create"},
                },
            },
        )

        apply_pfra_dag_policy(dag_mock)

        assert dag_mock.access_control == {
            "role_1": {
                "DAG Runs": {"can_create"},
                "DAGs": {"can_edit", "can_read"},
            },
        }

    def test_apply_pfra_dag_policy_access_control_already_has_this_role_and_dags_resource(self):
        dag_mock = mock.Mock(
            fileloc=os.path.join(settings.DAGS_FOLDER, "role_1/dag_1.py"),
            access_control={
                "role_1": {
                    "DAGs": {"can_create"},
                },
            },
        )

        apply_pfra_dag_policy(dag_mock)

        assert dag_mock.access_control == {
            "role_1": {
                "DAGs": {"can_create", "can_edit", "can_read"},
            },
        }

    def test_apply_pfra_dag_policy_dag_in_root_folder(self):
        dag_mock = mock.Mock(
            fileloc=os.path.join(settings.DAGS_FOLDER, "dag_1.py"),
            access_control=None,
        )

        apply_pfra_dag_policy(dag_mock)

        assert dag_mock.access_control == {}

    def test_apply_pfra_dag_policy_dag_in_folder_with_more_than_64_chars(self):
        folder_name = "a" * 65
        dag_mock = mock.Mock(
            fileloc=os.path.join(settings.DAGS_FOLDER, f"{folder_name}/dag_1.py"),
            access_control=None,
        )

        apply_pfra_dag_policy(dag_mock)

        assert dag_mock.access_control == {}
