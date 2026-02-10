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

import functools

from airflow.composer.patches.rbac.per_folder_roles_autoregistration import RBAC_AUTOREGISTER_PER_FOLDER_ROLES
from airflow.dag_processing import collection
from airflow.providers.fab.www.security_appless import ApplessAirflowSecurityManager


def patch():
    collection._sync_dag_perms = _composer_collection_sync_dag_perms(collection._sync_dag_perms)


def _composer_collection_sync_dag_perms(f):
    @functools.wraps(f)
    def wrapper(dag, session, *args, **kwargs):
        if RBAC_AUTOREGISTER_PER_FOLDER_ROLES:
            security_manager = ApplessAirflowSecurityManager(session=session)

            # Create roles in RBAC tables if they do not yet exist.
            for _role in dag.access_control:
                security_manager.add_role(_role)

        return f(dag, session, *args, **kwargs)

    return wrapper
