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
import tempfile
from typing import TYPE_CHECKING

import yaml
from kubernetes.client import CustomObjectsApi

from airflow.configuration import AIRFLOW_HOME, conf

if TYPE_CHECKING:
    from kubernetes.client import ApiClient

log = logging.getLogger(__name__)

POD_TEMPLATE_FILE = os.path.join(AIRFLOW_HOME, "composer_kubernetes_pod_template_file.yaml")
POD_TEMPLATE_FILE_REFRESH_INTERVAL = conf.getint(
    "kubernetes_executor", "pod_template_file_refresh_interval", fallback=60
)


def refresh_pod_template_file(api_client: ApiClient):
    """
    Refresh Composer pod template file used by KubernetesExecutor.

    The general idea of this method is to read current Airflow worker pod template and use it for
    KubernetesExecutor (with some adjustments - removing/updating specific fields).
    Prepared pod template is stored in yaml file (POD_TEMPLATE_FILE).

    :param api_client: k8s API client.
    """
    log.info("Refreshing Composer kubernetes pod template file")

    # Read Airflow worker pod template and do some adjustments to it, required to run it with
    # KubernetesExecutor. Note, that `kind` and `apiVersion` fields will be added by executor.
    kube_client = CustomObjectsApi(api_client=api_client)
    pod_template_dict = api_client.sanitize_for_serialization(
        kube_client.get_namespaced_custom_object(
            group="composer.cloud.google.com",
            version="v1beta1",
            plural="airflowworkersets",
            name="airflow-worker",
            namespace=os.environ.get("COMPOSER_VERSIONED_NAMESPACE"),
        )["spec"]["template"]
    )

    # As of 2021-11-15 only labels (one) are stored in metadata of template for worker pod, these labels are
    # used by selector in airflow-worker AirflowWorkerSet, so we remove it from pod template for
    # KubernetesExecutor.
    del pod_template_dict["metadata"]

    # We do not need liveness probe for main container.
    pod_template_dict["spec"]["containers"][0].pop("livenessProbe", None)
    # Never restart containers inside pod.
    pod_template_dict["spec"]["restartPolicy"] = "Never"

    # Add AIRFLOW_IS_K8S_EXECUTOR_POD environment variable for all containers inside pod. Note, that this
    # environment variable is automatically added by KubernetesExecutor for first container of task pod, here
    # we add it for all containers.
    for c in pod_template_dict["spec"]["containers"]:
        c.setdefault("env", [])
        c["env"].append({"name": "AIRFLOW_IS_K8S_EXECUTOR_POD", "value": "True"})

    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        f.write(yaml.dump(pod_template_dict))
    # Atomically override file. "os.rename" is bulletproof to race conditions such as another thread/process
    # will read file while current is overriding it (file handle will continue to refer to the original
    # version of the file).
    # https://stackoverflow.com/questions/2028874/what-happens-to-an-open-file-handle-on-linux-if-the-pointed-file-gets-moved-or-d
    os.rename(f.name, POD_TEMPLATE_FILE)


def get_task_run_command_from_args(args):
    """
    Return command to run Airflow task.

    :param args: list of arguments with command to run Airflow task.
    """
    # Escape all arguments and concatenate them into a string to be used as a command in bash.
    # https://stackoverflow.com/questions/6306386/how-can-i-escape-an-arbitrary-string-for-use-as-a-command-line-argument-in-bash
    return " ".join(["'{}'".format(str(arg).replace("'", r"'\''")) for arg in args])
