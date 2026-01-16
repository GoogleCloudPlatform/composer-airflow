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

import logging
import os

from airflow import settings
from airflow.configuration import conf
from airflow.providers.fab.www.security import permissions

log = logging.getLogger(__name__)

RBAC_AUTOREGISTER_PER_FOLDER_ROLES = conf.getboolean(
    "webserver", "rbac_autoregister_per_folder_roles", fallback=False
)


def apply_pfra_dag_policy(dag):
    """Add per-folder role to the "access_control" Dag property."""
    role = _get_dag_role(dag)

    access_control = dag.access_control or {}
    if role:
        access_control.setdefault(role, {})
        access_control[role].setdefault("DAGs", set())
        access_control[role]["DAGs"].update({permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ})
    # It is important to leave dag.access_control not None (e.g. {} in case of original dag.access_control is
    # None and per-folder role is None), to force Airflow sync Dag-level permissions (or rather clean up in
    # case of {}).
    dag.access_control = access_control


def _get_dag_role(dag):
    """Retrieve role name from Dag filepath."""
    # Check if the DAG is in a subfolder.
    dag_relative_filepath = os.path.relpath(dag.fileloc, settings.DAGS_FOLDER)
    if os.path.sep not in dag_relative_filepath:
        # DAGs located directly in the top-level DAGs folder are not auto-assigned to any per-folder role.
        return None

    # Use the subfolder name as the role name, if possible.
    dag_subfolder = dag_relative_filepath.split(os.path.sep)[0]
    if len(dag_subfolder) > 64:
        log.warning(
            "Folder name %s exceeds the maximum role name length of 64 characters, ignoring Per-folder Roles "
            "Registration for DAG in this folder",
            dag_subfolder,
        )
        return None

    return dag_subfolder
