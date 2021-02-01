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
"""Airflow per-folder RBAC.

This policy is enabled only when [webserver]rbac_autoregister_per_folder_roles
configuration property is True. It runs for every DAG processed by Airflow
scheduler, and implements per-folder role access control in Airflow,
which works in the following way:

    * If the DAG is located in a subfolder of the DAGs folder, and the subfolder
      name can be used as Airflow RBAC role name, then:

        * Use the subfolder name as the role name.
        * Add ACTION_CAN_EDIT and ACTION_CAN_READ permissions on the DAG to the
        role.

The policy runs in scheduler's DAG file processing subprocesses, and modifies
"access_control" property of the DAG.
"""

import logging
import os

from airflow import settings
from airflow.security.permissions import ACTION_CAN_EDIT, ACTION_CAN_READ

log = logging.getLogger(__file__)


def _role_from_filepath(dag_filepath):
    """Retrieves role name from DAG filepath."""
    # Check if the DAG is in a subfolder.
    dag_relative_filepath = os.path.relpath(dag_filepath, settings.DAGS_FOLDER)
    if os.path.sep not in dag_relative_filepath:
        # DAGs located directly in the top-level DAGs folder are not
        # auto-assigned to any per-folder role.
        return None

    # Use the subfolder name as the role name, if possible.
    dag_subfolder = dag_relative_filepath.split(os.path.sep)[0]
    if len(dag_subfolder) > 64:
        log.warning(
            "Folder name %s exceeds the maximum role name length of 64 "
            "characters, "
            "ignoring RBAC role autoregistration for DAG in this folder",
            dag_subfolder,
        )
        return None

    return dag_subfolder


def apply_dag_rbac_per_folder_policy(dag):
    """Adds per-folder role to the "access_control" DAG property."""
    role = _role_from_filepath(dag.fileloc)
    if not role:
        return

    # Merge per-folder permissions with existing.
    _access_control = dag.access_control or {}
    _access_control.setdefault(role, set())
    _access_control[role].update({ACTION_CAN_EDIT, ACTION_CAN_READ})
    dag.access_control = _access_control
