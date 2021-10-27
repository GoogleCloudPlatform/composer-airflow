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
import datetime
import os
from unittest import mock

import pytest
from celery import signals
from kubernetes.client import Configuration, models as k8s

from airflow import DAG, settings
from airflow.composer.airflow_local_settings import dag_policy, pod_mutation_hook
from airflow.configuration import conf
from airflow.security.permissions import ACTION_CAN_EDIT, ACTION_CAN_READ
from tests.test_utils.config import conf_vars


class TestAirflowLocalSettings:
    @conf_vars({("webserver", "rbac_autoregister_per_folder_roles"): "True"})
    def test_dag_rbac_per_folder_policy(self):
        role_a_dag = DAG(
            dag_id="role_a_dag",
            start_date=datetime.datetime(2017, 1, 1),
            schedule=datetime.timedelta(days=1),
        )
        role_a_dag.fileloc = os.path.join(settings.DAGS_FOLDER, "role_a/dag.py")
        role_b_dag = DAG(
            dag_id="role_b_dag",
            start_date=datetime.datetime(2017, 1, 1),
            access_control={
                "role_b": {"test_permission"},
                "admin": {"admin_permission"},
            },
            schedule=datetime.timedelta(days=1),
        )
        role_b_dag.fileloc = os.path.join(settings.DAGS_FOLDER, "role_b/dag.py")
        root_dag = DAG(
            dag_id="root_dag",
            start_date=datetime.datetime(2017, 1, 1),
            schedule=datetime.timedelta(days=1),
        )
        root_dag.fileloc = os.path.join(settings.DAGS_FOLDER, "dag.py")
        role_length_exceed_dag = DAG(
            dag_id="role_length_exceed_dag",
            start_date=datetime.datetime(2017, 1, 1),
            schedule=datetime.timedelta(days=1),
        )
        role_length_exceed_dag.fileloc = os.path.join(settings.DAGS_FOLDER, f"role_{'x' * 70}/dag.py")

        dag_policy(role_a_dag)
        dag_policy(role_b_dag)
        dag_policy(root_dag)
        dag_policy(role_length_exceed_dag)

        assert role_a_dag.access_control == {"role_a": {"DAGs": {ACTION_CAN_EDIT, ACTION_CAN_READ}}}
        assert role_b_dag.access_control == {
            "role_b": {"DAGs": {"test_permission", ACTION_CAN_EDIT, ACTION_CAN_READ}},
            "admin": {"DAGs": {"admin_permission"}},
        }
        assert root_dag.access_control is None
        assert role_length_exceed_dag.access_control is None

    @pytest.mark.parametrize(
        "composer_version, namespace, expected_mutated_namespace",
        [
            (
                "1.20.12",
                "default",
                "default",
            ),
            (
                "2.4.21",
                "default",
                "default",
            ),
            (
                "3.0.0-preview.0",
                "default",
                "composer-user-workloads",
            ),
        ],
    )
    @mock.patch(
        "airflow.composer.utils.get_composer_gke_cluster_host",
        mock.Mock(return_value="http://internal-cluster"),
    )
    @mock.patch.dict("os.environ", {"COMPOSER_GKE_LOCATION": "us-east1"})
    @mock.patch.dict("os.environ", {"GCP_TENANT_PROJECT": "test-project-234"})
    def test_pod_mutation_hook(self, composer_version, namespace, expected_mutated_namespace):
        Configuration.set_default(Configuration(host="http://internal-cluster"))
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(namespace=namespace),
            spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base")]),
        )

        with mock.patch.dict("os.environ", {"COMPOSER_VERSION": composer_version}):
            pod_mutation_hook(pod)

        assert pod.metadata.namespace == expected_mutated_namespace

    @mock.patch(
        "airflow.composer.utils.get_composer_gke_cluster_host",
        mock.Mock(return_value="http://internal-cluster"),
    )
    def test_pod_mutation_hook_external_gke_cluster(self):
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(namespace="default"),
            spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base")]),
        )
        Configuration.set_default(Configuration(host="http://external-cluster"))

        with mock.patch.dict("os.environ", {"COMPOSER_VERSION": "2.5.0-preview.0"}):
            pod_mutation_hook(pod)

        assert pod == k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(namespace="default"),
            spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base")]),
        )

    @mock.patch.dict("os.environ", {"COMPOSER_VERSION": "3.0.0"})
    @mock.patch(
        "airflow.composer.utils.get_composer_gke_cluster_host",
        mock.Mock(return_value="http://internal-cluster"),
    )
    @mock.patch("airflow.composer.kubernetes.utils.PodGenerator", autospec=True)
    @mock.patch("airflow.composer.kubernetes.utils._get_composer_serverless_pod_metadata", autospec=True)
    def test_pod_mutation_hook_serverless_internal_gke_cluster(
        self, get_composer_serverless_pod_metadata_mock, pod_generator_mock
    ):
        Configuration.set_default(Configuration(host="http://internal-cluster"))
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(namespace="n1"),
            spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base")]),
        )
        get_composer_serverless_pod_metadata_mock.side_effect = [k8s.V1ObjectMeta(namespace="n2")]

        def reconcile_metadata_side_effect(pod_metadata, composer_serverless_pod_metadata):
            assert pod_metadata == k8s.V1ObjectMeta(namespace="n1")
            assert composer_serverless_pod_metadata == k8s.V1ObjectMeta(namespace="n2")
            return k8s.V1ObjectMeta(namespace="n3")

        pod_generator_mock.reconcile_metadata.side_effect = reconcile_metadata_side_effect

        pod_mutation_hook(pod)

        assert pod.metadata == k8s.V1ObjectMeta(namespace="n3")

    @pytest.mark.parametrize(
        "composer_version, patch_fetch_container_logs_expected_calls_count",
        [
            ("2.1.10", 0),
            ("3.0.1", 1),
        ],
    )
    @mock.patch("airflow.composer.kubernetes.pod_manager.patch_fetch_container_logs", autospec=True)
    @mock.patch("airflow.composer.utils.get_composer_gke_cluster_host", autospec=True)
    @mock.patch("airflow.composer.kubernetes.utils.pod_mutation_hook_composer_serverless", autospec=True)
    def test_pod_mutation_hook_patch_fetch_container_logs(
        self,
        pod_mutation_hook_composer_serverless_mock,
        get_composer_gke_cluster_host_mock,
        patch_fetch_container_logs_mock,
        composer_version,
        patch_fetch_container_logs_expected_calls_count,
    ):
        pod_mutation_hook_composer_serverless_mock.return_value = mock.Mock()
        get_composer_gke_cluster_host_mock.return_value = mock.Mock()
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(namespace="n1"),
            spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base")]),
        )
        with mock.patch.dict("os.environ", {"COMPOSER_VERSION": composer_version}):
            pod_mutation_hook(pod=pod)

        assert patch_fetch_container_logs_mock.call_count == patch_fetch_container_logs_expected_calls_count

    @mock.patch.dict("os.environ", {"COMPOSER_VERSION": "3.0.0"})
    @mock.patch(
        "airflow.composer.airflow_local_settings.sys.argv",
        ["airflow", "scheduler"],
    )
    @mock.patch(
        "airflow.composer.kubernetes.utils.pod_mutation_hook_composer_serverless",
        autospec=True,
    )
    def test_pod_mutation_hook_scheduler(self, pod_mutation_hook_composer_serverless_mock):
        pod_mock = mock.MagicMock()

        pod_mutation_hook(pod_mock)

        pod_mutation_hook_composer_serverless_mock.assert_called_with(pod_mock)

    @pytest.mark.parametrize(
        (
            "composer_version, namespace, expected_mutated_namespace, "
            "env_vars, expected_env_vars, pod_name, expected_pod_name, args, expected_args"
        ),
        [
            (
                "2.4.21",
                "default",
                "default",
                [],
                [],
                "pod-name",
                "pod-name",
                ["worker"],
                ["worker"],
            ),
            (
                "3.0.0-preview.0",
                "default",
                "composer-user-workloads",
                [],
                [],
                "pod-name",
                "pod-name",
                ["worker"],
                ["worker"],
            ),
            (
                "2.4.21",
                "default",
                "default",
                [k8s.V1EnvVar(name="AIRFLOW_IS_K8S_EXECUTOR_POD", value=True)],
                [
                    k8s.V1EnvVar(name="AIRFLOW_IS_K8S_EXECUTOR_POD", value=True),
                    k8s.V1EnvVar(
                        name="AIRFLOW_K8S_EXECUTOR_POD_TASK_RUN_COMMAND",
                        value="'airflow' 'tasks' 'run' 'dag'\\''id'",
                    ),
                ],
                "pod-name-123",
                "airflow-k8s-worker-pod-name-123",
                ["airflow", "tasks", "run", "dag'id"],
                ["worker"],
            ),
            (
                "3.0.0-preview.0",
                "default",
                "composer-user-workloads",
                [k8s.V1EnvVar(name="AIRFLOW_IS_K8S_EXECUTOR_POD", value=True)],
                [
                    k8s.V1EnvVar(name="AIRFLOW_IS_K8S_EXECUTOR_POD", value=True),
                    k8s.V1EnvVar(
                        name="AIRFLOW_K8S_EXECUTOR_POD_TASK_RUN_COMMAND",
                        value="'airflow' 'tasks' 'run' 'dag'\\''id'",
                    ),
                ],
                "pod-name-123",
                "airflow-k8s-worker-pod-name-123",
                ["airflow", "tasks", "run", "dag'id"],
                ["worker"],
            ),
        ],
    )
    @mock.patch(
        "airflow.composer.utils.get_composer_gke_cluster_host",
        mock.Mock(return_value="http://internal-cluster"),
    )
    @mock.patch.dict("os.environ", {"COMPOSER_GKE_LOCATION": "us-east1"})
    @mock.patch.dict("os.environ", {"GCP_TENANT_PROJECT": "test-project-234"})
    def test_pod_mutation_hook_k8s_executor(
        self,
        composer_version,
        namespace,
        expected_mutated_namespace,
        env_vars,
        expected_env_vars,
        pod_name,
        expected_pod_name,
        args,
        expected_args,
    ):
        Configuration.set_default(Configuration(host="http://internal-cluster"))
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name=pod_name, namespace=namespace),
            spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base", env=env_vars, args=args)]),
        )

        with mock.patch.dict("os.environ", {"COMPOSER_VERSION": composer_version}):
            pod_mutation_hook(pod)

        assert pod.metadata.namespace == expected_mutated_namespace
        assert pod.metadata.name == expected_pod_name
        assert pod.spec.containers[0].env == expected_env_vars
        assert pod.spec.containers[0].args == expected_args

    def test_pod_mutation_hook_k8s_executor_long_name(self):
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name="A" * 100),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        args=[],
                        env=[k8s.V1EnvVar(name="AIRFLOW_IS_K8S_EXECUTOR_POD", value=True)],
                    )
                ]
            ),
        )

        pod_mutation_hook(pod)

        assert len(pod.metadata.name) == 63
        # Pod name should start with airflow-k8s-worker and has random suffix of the 8 characters length.
        assert pod.metadata.name[:-8] == f"airflow-k8s-worker-{'A' * 35}-"


def test_setup_logging_on_celeryd_init():
    from airflow.composer import airflow_local_settings

    conf_mock = mock.Mock()
    conf_mock.broker_connection_retry = True

    signals.celeryd_init.send(
        sender=None,
        conf=conf_mock,
    )

    assert conf_mock.worker_log_format == conf.get("logging", "LOG_FORMAT")
    assert conf_mock.broker_connection_retry_on_startup is True
