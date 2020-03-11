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
from airflow.configuration import conf

from airflow.exceptions import DagNotFound, DagRunNotFound
from airflow.models import DagBag
from airflow.utils.log.logging_mixin import LoggingMixin


def get_dag_run_state(dag_id, execution_date):
    """Return the task object identified by the given dag_id and task_id."""
    logger = LoggingMixin()
    logger.log.info("Called get_dag_run_state for: %s", dag_id)

    dagbag = DagBag(store_serialized_dags=conf.getboolean('core', 'store_serialized_dags', fallback=False))
    dag = dagbag.get_dag(dag_id)

    # Check DAG exists.
    if dag is None:
        error_message = "Dag id {} not found".format(dag_id)
        raise DagNotFound(error_message)

    # Get DagRun object and check that it exists
    dagrun = dag.get_dagrun(execution_date=execution_date)
    if not dagrun:
        error_message = ('Dag Run for date {} not found in dag {}'
                         .format(execution_date, dag_id))
        raise DagRunNotFound(error_message)

    return {'state': dagrun.get_state()}
