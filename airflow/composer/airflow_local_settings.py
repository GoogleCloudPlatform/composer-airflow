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
"""Airflow local settings."""
from __future__ import annotations

import logging

from kubernetes.client import Configuration, models as k8s

from airflow.composer.kubernetes.utils import pod_mutation_hook_composer_serverless
from airflow.composer.utils import get_composer_gke_cluster_host, is_serverless_composer

log = logging.getLogger(__file__)


def dag_policy(dag):
    """Applies per-DAG policy."""
    # Avoid circular imports by moving imports inside method.
    from airflow.composer.dag_rbac_per_folder import apply_dag_rbac_per_folder_policy
    from airflow.configuration import conf

    if conf.getboolean("webserver", "rbac_autoregister_per_folder_roles", fallback=False):
        apply_dag_rbac_per_folder_policy(dag)


def pod_mutation_hook(pod: k8s.V1Pod):
    # For Composer serverless in case of running pod by KPO or KubernetesExecutor we should adjust pod spec.
    # Refer to go/composer25-kpo-k8s-executor for details.
    # Note, that we check below if cluster host where pod will be deployed is a Composer GKE cluster host, to
    # account for GKEStartPodOperator which run pods in external GKE cluster.
    if is_serverless_composer() and Configuration.get_default_copy().host == get_composer_gke_cluster_host():
        log.info("Modifying pod spec")
        pod_mutation_hook_composer_serverless(pod)
