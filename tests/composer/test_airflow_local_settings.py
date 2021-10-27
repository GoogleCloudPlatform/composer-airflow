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
from __future__ import annotations

import datetime
import os
import unittest
from unittest import mock

from kubernetes.client import Configuration, models as k8s
from parameterized import parameterized

from airflow import settings
from airflow.composer.airflow_local_settings import dag_policy, pod_mutation_hook
from airflow.models import DAG
from airflow.security.permissions import ACTION_CAN_EDIT, ACTION_CAN_READ
from tests.test_utils.config import conf_vars


class TestAirflowLocalSettings(unittest.TestCase):
    @conf_vars({("webserver", "rbac_autoregister_per_folder_roles"): "True"})
    def test_dag_rbac_per_folder_policy(self):
        role_a_dag = DAG(
            dag_id="role_a_dag",
            start_date=datetime.datetime(2017, 1, 1),
        )
        role_a_dag.fileloc = os.path.join(settings.DAGS_FOLDER, "role_a/dag.py")
        role_b_dag = DAG(
            dag_id="role_b_dag",
            start_date=datetime.datetime(2017, 1, 1),
            access_control={
                "role_b": {"test_permission"},
                "admin": {"admin_permission"},
            },
        )
        role_b_dag.fileloc = os.path.join(settings.DAGS_FOLDER, "role_b/dag.py")
        root_dag = DAG(
            dag_id="root_dag",
            start_date=datetime.datetime(2017, 1, 1),
        )
        root_dag.fileloc = os.path.join(settings.DAGS_FOLDER, "dag.py")
        role_length_exceed_dag = DAG(
            dag_id="role_length_exceed_dag",
            start_date=datetime.datetime(2017, 1, 1),
        )
        role_length_exceed_dag.fileloc = os.path.join(settings.DAGS_FOLDER, f"role_{'x' * 70}/dag.py")

        dag_policy(role_a_dag)
        dag_policy(role_b_dag)
        dag_policy(root_dag)
        dag_policy(role_length_exceed_dag)

        assert role_a_dag.access_control == {"role_a": {ACTION_CAN_EDIT, ACTION_CAN_READ}}
        assert role_b_dag.access_control == {
            "role_b": {"test_permission", ACTION_CAN_EDIT, ACTION_CAN_READ},
            "admin": {"admin_permission"},
        }
        assert root_dag.access_control is None
        assert role_length_exceed_dag.access_control is None

    @parameterized.expand(
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
                "2.5.0-preview.0",
                "default",
                "composer-user-workloads",
            ),
        ]
    )
    @mock.patch(
        "airflow.composer.airflow_local_settings.get_composer_gke_cluster_host",
        mock.Mock(return_value="http://internal-cluster"),
    )
    @mock.patch.dict("os.environ", {"COMPOSER_GKE_LOCATION": "us-east1"})
    @mock.patch.dict("os.environ", {"GCP_PROJECT": "test-project-234"})
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
        "airflow.composer.airflow_local_settings.get_composer_gke_cluster_host",
        mock.Mock(return_value="http://internal-cluster"),
    )
    def test_pod_mutation_hook_external_gke_cluster(self):
        pod = k8s.V1Pod(metadata=k8s.V1ObjectMeta(namespace="default"))
        Configuration.set_default(Configuration(host="http://external-cluster"))

        with mock.patch.dict("os.environ", {"COMPOSER_VERSION": "2.5.0-preview.0"}):
            pod_mutation_hook(pod)

        assert pod == k8s.V1Pod(metadata=k8s.V1ObjectMeta(namespace="default"))

    @mock.patch.dict("os.environ", {"COMPOSER_VERSION": "2.5.0"})
    @mock.patch(
        "airflow.composer.airflow_local_settings.get_composer_gke_cluster_host",
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
