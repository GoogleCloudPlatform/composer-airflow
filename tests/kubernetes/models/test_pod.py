# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import unittest
from tests.compat import mock
from kubernetes.client import ApiClient
import kubernetes.client.models as k8s
from airflow.kubernetes.pod import Port
from airflow.kubernetes.pod_generator import PodGenerator
from airflow.kubernetes.k8s_model import append_to_pod


class TestPod(unittest.TestCase):

    def test_port_to_k8s_client_obj(self):
        port = Port('http', 80)
        self.assertEqual(
            port.to_k8s_client_obj(),
            k8s.V1ContainerPort(
                name='http',
                container_port=80
            )
        )

    @mock.patch('uuid.uuid4')
    def test_port_attach_to_pod(self, mock_uuid):
        import uuid
        static_uuid = uuid.UUID('cf4a56d2-8101-4217-b027-2af6216feb48')
        mock_uuid.return_value = static_uuid
        pod = PodGenerator(image='airflow-worker:latest', name='base').gen_pod()
        ports = [
            Port('https', 443),
            Port('http', 80)
        ]
        k8s_client = ApiClient()
        result = append_to_pod(pod, ports)
        result = k8s_client.sanitize_for_serialization(result)
        self.assertEqual({
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {'name': 'base-' + static_uuid.hex},
            'spec': {
                'containers': [{
                    'args': [],
                    'command': [],
                    'env': [],
                    'envFrom': [],
                    'image': 'airflow-worker:latest',
                    'name': 'base',
                    'ports': [{
                        'name': 'https',
                        'containerPort': 443
                    }, {
                        'name': 'http',
                        'containerPort': 80
                    }],
                    'volumeMounts': [],
                }],
                'hostNetwork': False,
                'imagePullSecrets': [],
                'volumes': []
            }
        }, result)

    def test_to_v1_pod(self):
        from airflow.kubernetes_deprecated.pod import Pod as DeprecatedPod
        from airflow.kubernetes.volume import Volume
        from airflow.kubernetes.volume_mount import VolumeMount
        from airflow.kubernetes.pod import Resources

        pod = DeprecatedPod(
            image="foo",
            name="bar",
            namespace="baz",
            image_pull_policy="Never",
            envs={"test_key": "test_value"},
            cmds=["airflow"],
            resources=Resources(
                request_memory="1G",
                request_cpu="100Mi",
                limit_gpu="100G"
            ),
            volumes=[Volume(name="foo", configs={})],
            volume_mounts=[VolumeMount(name="foo", mount_path="/mnt", sub_path="/", read_only=True)]
        )

        k8s_client = ApiClient()

        result = pod.to_v1_kubernetes_pod()
        result = k8s_client.sanitize_for_serialization(result)

        expected = \
            {
                'metadata':
                    {
                        'labels': {},
                        'name': 'bar',
                        'namespace': 'baz'
                    },
                'spec':
                    {'containers':
                        [
                            {
                                'args': [],
                                'command': ['airflow'],
                                'env': [{'name': 'test_key', 'value': 'test_value'}],
                                'image': 'foo',
                                'imagePullPolicy': 'Never',
                                'name': 'base',
                                'volumeMounts':
                                    [
                                        {
                                            'mountPath': '/mnt',
                                            'name': 'foo',
                                            'readOnly': True, 'subPath': '/'
                                        }
                                    ],  # noqa
                                'resources':
                                    {
                                        'limits':
                                            {
                                                'cpu': None,
                                                'memory': None,
                                                'nvidia.com/gpu': '100G',
                                                'ephemeral-storage': None
                                            },
                                        'requests':
                                            {
                                                'cpu': '100Mi',
                                                'memory': '1G',
                                                'ephemeral-storage': None
                                            }
                                }
                            }
                        ],
                        'hostNetwork': False,
                        'tolerations': [],
                        'volumes': [
                            {'name': 'foo'}
                        ]
                     }
            }
        self.maxDiff = None
        self.assertEquals(expected, result)
