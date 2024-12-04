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
import logging
import sys

from celery.signals import celeryd_init
from kubernetes.client import Configuration, models as k8s

log = logging.getLogger(__file__)


def dag_policy(dag):
    """Applies per-DAG policy."""
    # Avoid circular imports by moving imports inside method.
    from airflow.composer.dag_rbac_per_folder import apply_dag_rbac_per_folder_policy
    from airflow.configuration import conf

    if conf.getboolean("webserver", "rbac_autoregister_per_folder_roles", fallback=False):
        apply_dag_rbac_per_folder_policy(dag)


def pod_mutation_hook(pod: k8s.V1Pod):
    # Avoid circular imports by moving imports inside method.
    from airflow.composer.kubernetes.executor import get_task_run_command_from_args
    from airflow.composer.kubernetes.pod_manager import patch_fetch_container_logs
    from airflow.composer.kubernetes.utils import pod_mutation_hook_composer_serverless
    from airflow.composer.utils import get_composer_gke_cluster_host, is_serverless_composer
    from airflow.providers.cncf.kubernetes.kubernetes_helper_functions import add_unique_suffix

    if is_serverless_composer():
        patch_fetch_container_logs()
    # For Composer serverless in case of running pod by KPO or KubernetesExecutor we should adjust pod spec.
    # Refer to go/composer25-kpo-k8s-executor for details.
    # Note, that we check below if cluster host where pod will be deployed is a Composer GKE cluster host, to
    # account for GKEStartPodOperator which run pods in external GKE cluster.
    # In case of scheduler (KubernetesExecutor) the logic with checking host doesn't work, but we can check
    # if it is scheduler by inspecting sys.argv.
    is_scheduler = "scheduler" in sys.argv[1]
    if is_serverless_composer() and (
        is_scheduler or Configuration.get_default_copy().host == get_composer_gke_cluster_host()
    ):
        log.info("Modifying pod spec")
        pod_mutation_hook_composer_serverless(pod)

    # Adjust pod spec for KubernetesExecutor pods.
    if (
        pod.spec.containers and
        pod.spec.containers[0].env and
        any(env_var.name == "AIRFLOW_IS_K8S_EXECUTOR_POD" for env_var in pod.spec.containers[0].env)
    ):
        if not pod.metadata.name.startswith("airflow-k8s-worker"):
            # The pod name should be at maximum 63 characters length.
            # https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-label-names
            max_len = 63
            new_name = "airflow-k8s-worker-" + pod.metadata.name
            # The pod names generated are already unique, but if we have to truncate them,
            # they will lose their unique suffix, so we add it again
            if len(new_name) > max_len:
                new_name = add_unique_suffix(name=new_name, rand_len=8, max_len=max_len)
            pod.metadata.name = new_name

        container = pod.spec.containers[0]
        args = container.args
        container.env.append(
            k8s.V1EnvVar(
                name="AIRFLOW_K8S_EXECUTOR_POD_TASK_RUN_COMMAND",
                value=get_task_run_command_from_args(args),
            )
        )
        # Pass ["worker"] as args in base container to bypass GKE policy exemptor.
        container.args = ["worker"]


@celeryd_init.connect
def setup_logging_on_celeryd_init(**kwargs):
    """Customize Celery logging configuration as per Composer needs."""
    from airflow.configuration import conf

    # Apply same format for Celery logs as we have for all other logs.
    # From https://github.com/celery/celery/issues/3599.
    kwargs["conf"].worker_log_format = conf.get("logging", "LOG_FORMAT")

    # Set broker_connection_retry_on_startup as broker_connection_retry to suppress CPendingDeprecationWarning
    # coming from Celery:
    # "The broker_connection_retry configuration setting will no longer determine whether broker connection
    # retries are made during startup in Celery 6.0 and above. If you wish to retain the existing behavior for
    # retrying connections on startup, you should set broker_connection_retry_on_startup to True."
    kwargs["conf"].broker_connection_retry_on_startup = kwargs["conf"].broker_connection_retry
