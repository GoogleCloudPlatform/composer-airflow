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

from airflow.exceptions import AirflowException

from airflow import example_dags as example_dags_module
from airflow.models import DagBag
from airflow.models.dagcode import DagCode
# To move it to a shared module.
from airflow.utils.file import open_maybe_zipped
from airflow.utils.session import create_session
from mock import patch
from tests.test_utils.db import clear_db_dag_codes


def make_example_dags(module):
    """Loads DAGs from a module for test."""
    dagbag = DagBag(module.__path__[0])
    return dagbag.dags


class TestDagCode(unittest.TestCase):
    """Unit tests for DagCode."""

    def setUp(self):
        clear_db_dag_codes()

    def tearDown(self):
        clear_db_dag_codes()

    def _write_example_dags(self):
        example_dags = make_example_dags(example_dags_module)
        for dag in example_dags.values():
            DagCode(dag.fileloc).sync_to_db()
        return example_dags

    def test_sync_to_db(self):
        """Dag code can be written into database."""
        example_dags = self._write_example_dags()

        self._compare_example_dags(example_dags)

    def _compare_example_dags(self, example_dags):
        with create_session() as session:
            for dag in example_dags.values():
                self.assertTrue(DagCode.has_dag(dag.fileloc))
                dag_fileloc_hash = DagCode.dag_fileloc_hash(dag.fileloc)
                result = session.query(
                    DagCode.fileloc, DagCode.fileloc_hash, DagCode.source_code) \
                    .filter(DagCode.fileloc == dag.fileloc) \
                    .filter(DagCode.fileloc_hash == dag_fileloc_hash) \
                    .one()

                self.assertEqual(result.fileloc, dag.fileloc)
                with open_maybe_zipped(dag.fileloc, 'r') as source:
                    source_code = source.read()
                self.assertEqual(result.source_code, source_code)

    @patch.object(DagCode, 'dag_fileloc_hash')
    def test_detecting_duplicate_key(self, mock_hash):
        """Dag code detects duplicate key."""
        mock_hash.return_value = 0

        with self.assertRaisesRegex(AirflowException, 'causes a hash collision in the database'):
            self._write_example_dags()
