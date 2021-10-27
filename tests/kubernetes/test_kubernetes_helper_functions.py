#
# Copyright 2021 Google LLC
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
from unittest import TestCase

from airflow.kubernetes.kubernetes_helper_functions import create_pod_id


class TestKubernetesHelperFunctions(TestCase):
    def test_create_pod_id(self):
        assert create_pod_id('test-dag', 'task1') == 'airflow-k8s-worker-testdagtask1'
