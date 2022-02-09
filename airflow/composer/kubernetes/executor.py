import logging
import os
import tempfile

import yaml
from kubernetes.client import ApiClient, AppsV1Api, CustomObjectsApi

from airflow.composer.utils import is_composer_v1
from airflow.configuration import AIRFLOW_HOME, conf

POD_TEMPLATE_FILE = os.path.join(AIRFLOW_HOME, "composer_kubernetes_pod_template_file.yaml")
POD_TEMPLATE_FILE_REFRESH_INTERVAL = conf.getint(
    "kubernetes", "pod_template_file_refresh_interval", fallback=60
)
AIRFLOW_WORKER = "airflow-worker"
COMPOSER_VERSIONED_NAMESPACE = os.environ.get("COMPOSER_VERSIONED_NAMESPACE")

log = logging.getLogger(__file__)


def refresh_pod_template_file(api_client: ApiClient):
    """Refreshes Composer pod template file used by KubernetesExecutor.

    The general idea of this method is to read current Airflow worker pod template and prepare it to be
    used by KubernetesExecutor by adjusting some fields.
    The logic differs for Composer 1 and Composer 2, refer to the code for details. The general approach is to
    take existing template and remove/update specific fields.
    Prepared pod template is stored in yaml file (POD_TEMPLATE_FILE).

    :param api_client: k8s API client.
    :type api_client: ApiClient
    """
    log.info("Refreshing Composer kubernetes pod template file")
    if not COMPOSER_VERSIONED_NAMESPACE:
        log.info(
            "Skipping to refresh pod template file due to absence of Composer namespace environment variable"
        )
        return

    # Read Airflow worker pod template and do some adjustments to it required to run it with
    # KubernetesExecutor. Note, that kind and apiVersion fields will be added by executor.
    if is_composer_v1():
        kube_client = AppsV1Api(api_client=api_client)
        pod_template_dict = api_client.sanitize_for_serialization(
            kube_client.read_namespaced_deployment(AIRFLOW_WORKER, COMPOSER_VERSIONED_NAMESPACE).spec.template
        )

        # As of 2021-11-01 only labels (one) are stored in metadata of template
        # for worker pod, these labels are used by selector in airflow-worker
        # deployment, so we remove it from pod template for KubernetesExecutor.
        del pod_template_dict["metadata"]
        # As of 2021-11-01 the affinity field in worker pod is used for the
        # purpose of even distribution of them among nodes in GKE cluster, we
        # shouldn't use the same affinity value to not interfere with pods of
        # airflow-worker deployment.
        del pod_template_dict["spec"]["affinity"]
    else:
        kube_client = CustomObjectsApi(api_client=api_client)
        pod_template_dict = api_client.sanitize_for_serialization(
            kube_client.get_namespaced_custom_object(
                group="composer.cloud.google.com",
                version="v1beta1",
                plural="airflowworkersets",
                name=AIRFLOW_WORKER,
                namespace=COMPOSER_VERSIONED_NAMESPACE,
            )["spec"]["template"]
        )

        # As of 2021-11-15 only labels (one) are stored in metadata of template
        # for worker pod, these labels are used by selector in airflow-worker
        # AirflowWorkerSet, so we remove it from pod template for KubernetesExecutor.
        del pod_template_dict["metadata"]

    # We do not need liveness probe for main container.
    del pod_template_dict["spec"]["containers"][0]["livenessProbe"]
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
    """Returns command to run Airflow task.

    :param args: list of arguments with command to run Airflow task.
    :type args: List[str]
    """
    # Escape all arguments and concatenate them into string to be used as a command in bash.
    # https://stackoverflow.com/questions/6306386/how-can-i-escape-an-arbitrary-string-for-use-as-a-command-line-argument-in-bash
    return " ".join(["'{}'".format(str(arg).replace("'", r"'\''")) for arg in args])
