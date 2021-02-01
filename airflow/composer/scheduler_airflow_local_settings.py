#
# Copyright 2020 Google LLC
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
"""Airflow local settings for scheduler with cluster policy definition."""
_dag_to_filepath = {}


def task_policy(task):
    """Applies per-task policy."""
    # Avoid circular imports by moving imports inside method.
    from airflow.composer.rbac_role_autoregistration import apply_dag_role_registration_policy
    from airflow.configuration import conf

    global _dag_to_filepath  # pylint: disable=global-statement

    if conf.getboolean("webserver", "rbac_autoregister_per_folder_roles", fallback=False):
        dag = task.dag
        if _dag_to_filepath.get(dag.dag_id) != dag.full_filepath:
            apply_dag_role_registration_policy(dag)
            _dag_to_filepath[dag.dag_id] = dag.full_filepath
