# -*- coding: utf-8 -*-
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
"""Airflow RBAC per-folder role autoregistration.

This cluster policy is enabled only when both [webserver]rbac and
[webserver]rbac_autoregister_per_folder_roles configuration properties are True.
It runs for every DAG processed by Airflow scheduler, and implements per-folder
role autoregistration in Airflow RBAC, which works in the following way:

    * If the DAG is located in a subfolder of the DAGs folder, and the subfolder
      name can be used as Airflow RBAC role name, then:

        * Use the subfolder name as the role name.
        * Create the role in Airflow RBAC if it doesn't exist yet.
        * Add ACTION_CAN_EDIT and ACTION_CAN_READ permissions on the DAG to the role.

The policy runs in scheduler's DAG file processing subprocesses, and uses shared
objects from the main scheduler process.
"""

import contextlib
import logging
import os

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.security.permissions import ACTION_CAN_EDIT, ACTION_CAN_READ
from airflow.www.app import cached_app


log = logging.getLogger(__file__)


@contextlib.contextmanager
def _acquire_lock(lock):
    """Acquires and releases the given lock."""
    acquired = lock.acquire(timeout=30)
    if not acquired:
        raise AirflowException(
            "Unable to acquire lock for synchronizing DAG permissions")
    try:
        yield
    finally:
        lock.release()


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
            dag_subfolder)
        return None

    return dag_subfolder


def _synchronize_dag_role(security_manager, dag, role):
    """Synchronize the role and DAG access control."""

    # Create the role in RBAC tables if it doesn't exist.
    security_manager.add_role(role)

    # Specify DAG access control and sync it in RBAC tables.
    access_control = {
        role: {ACTION_CAN_EDIT, ACTION_CAN_READ},
    }
    security_manager.sync_perm_for_dag(dag.dag_id, access_control)

    log.info("RBAC role %s autoregistered for DAG %s", role, dag.dag_id)


def apply_dag_role_registration_policy(dag):
    """Applies per-DAG policy which registers DAG role based on folder name."""

    # Try to get the role name from the DAG filepath.
    role = _role_from_filepath(dag.full_filepath)
    if not role:
        return

    security_manager = cached_app().appbuilder.sm

    # Lock is required for exclusive access to both:
    # - dag_to_role dictionary, which stores already registered DAG roles
    # - security_manager object, which operates on the Airflow database
    with _acquire_lock(security_manager.lock):
        if security_manager.dag_to_role.get(dag.dag_id) != role:
            _synchronize_dag_role(security_manager, dag, role)
            security_manager.dag_to_role[dag.dag_id] = role
