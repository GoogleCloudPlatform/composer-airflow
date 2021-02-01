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

"""Scheduler command"""
import signal

import daemon
from daemon.pidfile import TimeoutPIDLockFile

from airflow import settings
from airflow.jobs.scheduler_job import SchedulerJob
from airflow.utils import cli as cli_utils
from airflow.utils.cli import process_subdir, setup_locations, setup_logging, sigint_handler, sigquit_handler


@cli_utils.action_logging
def scheduler(args):
    """Starts Airflow Scheduler"""
    print(settings.HEADER)

    from multiprocessing import Lock, Manager
    import os
    from airflow.www.app import cached_app
    from airflow.configuration import conf
    if conf.getboolean(
        'webserver', 'rbac_autoregister_per_folder_roles', fallback=False):
        # Prepare appbuilder instance to be shared across DAG loading processes.
        # It will be used for configuring per-folder DAG grouping roles.
        os.environ['SKIP_DAGS_PARSING'] = 'True'
        appbuilder = cached_app().appbuilder
        os.environ.pop('SKIP_DAGS_PARSING')
        appbuilder.sm.lock = Lock()
        appbuilder.sm.dag_to_role = Manager().dict()

    job = SchedulerJob(
        subdir=process_subdir(args.subdir),
        num_runs=args.num_runs,
        do_pickle=args.do_pickle,
    )

    if args.daemon:
        pid, stdout, stderr, log_file = setup_locations(
            "scheduler", args.pid, args.stdout, args.stderr, args.log_file
        )
        handle = setup_logging(log_file)
        stdout = open(stdout, 'w+')
        stderr = open(stderr, 'w+')

        ctx = daemon.DaemonContext(
            pidfile=TimeoutPIDLockFile(pid, -1),
            files_preserve=[handle],
            stdout=stdout,
            stderr=stderr,
        )
        with ctx:
            job.run()

        stdout.close()
        stderr.close()
    else:
        signal.signal(signal.SIGINT, sigint_handler)
        signal.signal(signal.SIGTERM, sigint_handler)
        signal.signal(signal.SIGQUIT, sigquit_handler)
        job.run()
