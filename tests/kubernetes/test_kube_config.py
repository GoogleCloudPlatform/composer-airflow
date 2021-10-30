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

from parameterized import parameterized

from airflow.kubernetes.kube_config import KubeConfig
from tests.test_utils.config import conf_vars


class TestKubeConfig(TestCase):
    @parameterized.expand(
        [
            ("repo", "tag", "repo:tag"),
            ("", "", ""),
        ]
    )
    def test_kube_config_kube_image(
        self,
        property_worker_container_repository,
        property_worker_container_tag,
        expected_kube_config_kube_image,
    ):
        with conf_vars(
            {
                ("kubernetes", "worker_container_repository"): property_worker_container_repository,
                ("kubernetes", "worker_container_tag"): property_worker_container_tag,
            }
        ):
            kube_config = KubeConfig()
            assert kube_config.kube_image == expected_kube_config_kube_image
