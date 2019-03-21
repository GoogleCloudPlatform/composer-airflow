# -*- coding: utf-8 -*-
#
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

from tests.contrib.utils.gcp_authenticator import GCP_AI_KEY
from tests.contrib.operators.test_gcp_vision_operator_system_helper import GCPVisionTestHelper
from tests.contrib.utils.base_gcp_system_test_case import SKIP_TEST_WARNING, DagGcpSystemTestCase

VISION_HELPER = GCPVisionTestHelper()


@unittest.skipIf(DagGcpSystemTestCase.skip_check(GCP_AI_KEY), SKIP_TEST_WARNING)
class CloudVisionExampleDagsSystemTest(DagGcpSystemTestCase):
    def __init__(self, method_name='runTest'):
        super(CloudVisionExampleDagsSystemTest, self).__init__(
            method_name, dag_name='example_gcp_vision.py', gcp_key=GCP_AI_KEY
        )

    def setUp(self):
        super(CloudVisionExampleDagsSystemTest, self).setUp()
        self.gcp_authenticator.gcp_authenticate()
        try:
            VISION_HELPER.create_bucket()
        finally:
            self.gcp_authenticator.gcp_revoke_authentication()

    def tearDown(self):
        self.gcp_authenticator.gcp_authenticate()
        try:
            VISION_HELPER.delete_bucket()
        finally:
            self.gcp_authenticator.gcp_revoke_authentication()
        super(CloudVisionExampleDagsSystemTest, self).tearDown()

    def test_run_example_gcp_vision_autogenerated_id_dag(self):
        self._run_dag('example_gcp_vision_autogenerated_id')

    def test_run_example_gcp_vision_explicit_id_dag(self):
        self._run_dag('example_gcp_vision_explicit_id')

    def test_run_example_gcp_vision_annotate_image_dag(self):
        self._run_dag('example_gcp_vision_annotate_image')
