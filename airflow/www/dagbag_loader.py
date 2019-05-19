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
#
import copy
import dill
import threading
import time
from multiprocessing import Process
from multiprocessing import Queue

from airflow import configuration as conf
from airflow import models
from airflow import settings
from airflow.utils.timeout import timeout


def read_config(field, default):
    try:
        return conf.getint('webserver', field)
    except Exception:
        return default


# Interval to send dagbag back to main process.
DAGBAG_SYNC_INTERVAL = read_config('dagbag_sync_interval', 10)
COLLECT_DAGS_INTERVAL = read_config('collect_dags_interval', 30)


class _DagBag(models.DagBag):
    """
    A wrapper of models.DagBag without calling collect_dags during initialization.
    """

    def __init__(self, dag_folder=None):
        # do not use default arg in signature, to fix import cycle on plugin load
        dag_folder = dag_folder or settings.DAGS_FOLDER
        self.log.info("Filling up the DagBag from %s", dag_folder)
        self.dag_folder = dag_folder
        self.dags = {}
        # the file's last modified timestamp when we last read it
        self.file_last_changed = {}
        self.executor = None
        self.import_errors = {}
        self.has_logged = False


def _create_dagbag(dag_folder, queue):
    """A process that creates, updates, and sync dagbag in background."""

    # Some customized fields of an operator, e.g., functions, may not be picklable.
    # So we stringify all fields of each task except for the attributes of BaseOperator
    # (airflow/airflow/models.py), and assume that Web UI only use these fields.
    _fields_to_keep = set((
        'task_id', 'owner', 'email', 'email_on_retry', 'email_on_failure', 'retries',
        'retry_delay', 'retry_exponential_backoff', 'max_retry_delay', 'start_date', 'end_date',
        'schedule_interval', 'depends_on_past', 'wait_for_downstream', 'dag', 'params',
        'default_args', 'adhoc', 'priority_weight', 'weight_rule', 'queue', 'pool', 'sla',
        'execution_timeout', 'trigger_rule', 'resources', 'run_as_user', 'task_concurrency',
        'executor_config', 'inlets', 'outlets', '_upstream_task_ids', '_downstream_task_ids',
        '_inlets', '_outlets', '_comps', '_dag'))

    def _stringify_dag(dag):
        """stringifies fields that may be not picklable."""
        # Assumes that Web UI never uses these fields.
        dag.user_defined_macros = str(dag.user_defined_macros)
        dag.user_defined_filters = str(dag.user_defined_filters)
        dag.sla_miss_callback = str(dag.sla_miss_callback)
        dag.on_success_callback = str(dag.on_success_callback)
        dag.on_failure_callback = str(dag.on_failure_callback)
        for task in dag.task_dict.values():
            for k, v in task.__dict__.items():
                if not (k in _fields_to_keep or type(v) in (int, bool, float, str)):
                    # Makes anything else a string to be displayed in web UI.
                    task.__dict__[k] = dill.source.getsource(v) if callable(v) else str(v)

    def _send_dagbag(dagbag, queue):
        """A thread that sends dags."""
        while True:
            dags = copy.deepcopy(dagbag.dags)
            [_stringify_dag(dag) for dag in dags.values()]
            queue.put(
                {
                    'dags': dags,
                    'file_last_changed': dagbag.file_last_changed,
                    'import_errors': dagbag.import_errors
                }
            )
            time.sleep(DAGBAG_SYNC_INTERVAL)

    dagbag = _DagBag(dag_folder)
    thread = threading.Thread(target=_send_dagbag, args=(dagbag, queue))
    thread.daemon = True
    thread.start()
    while True:
        start_time = time.time()
        dagbag.collect_dags(dag_folder,
                            include_examples=conf.getboolean('core', 'LOAD_EXAMPLES'))
        time.sleep(max(0, COLLECT_DAGS_INTERVAL - (time.time() - start_time)))


def create_async_dagbag(dag_folder):
    """
    It creates a new process to collect DAGs with a sender thread puts updated DAGs
    on a queue. The main process creates a receiver thread to get DAGs and update
    the DagBag. All new processe and threads are daemon so the main process never
    waits for them at exiting time.
    """
    def _receive_dagbag(dagbag, queue):
        """A thread that receives updated dagbag."""
        while True:
            for k, v in copy.deepcopy(queue.get()).items():
                dagbag.__dict__[k] = v

    dagbag = _DagBag(dag_folder)
    queue = Queue(maxsize=1)
    process = Process(target=_create_dagbag, args=(dag_folder, queue))
    process.daemon = True
    thread = threading.Thread(target=_receive_dagbag, args=(dagbag, queue))
    thread.daemon = True
    process.start()
    thread.start()
    return dagbag
