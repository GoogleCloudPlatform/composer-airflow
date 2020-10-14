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
import six

from airflow import settings
from airflow.utils import dagbag_loader

if six.PY2:
    # Need `assertRegex` back-ported from unittest2
    import unittest2 as unittest
else:
    import unittest


class AsyncDagBagLoaderTest(unittest.TestCase):

    def test_async_dagbag_loader(self):
        dagbag = dagbag_loader.create_async_dagbag(settings.DAGS_FOLDER)
        dag = dagbag.get_dag("example_bash_operator")
        self.assertEqual(dag.dag_id, "example_bash_operator")
