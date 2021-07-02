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
import datetime
import itertools
import logging
import multiprocessing
import os
import signal
import sys
import time
from collections import defaultdict
from datetime import timedelta
from typing import DefaultDict, Dict, Iterable, List, Optional, Set, Tuple

from sqlalchemy import and_, func, not_, or_, tuple_
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import load_only, selectinload
from sqlalchemy.orm.session import Session, make_transient
from sqlalchemy.sql import expression

from airflow import models, settings
from airflow.configuration import conf
from airflow.dag_processing.manager import DagFileProcessorAgent
from airflow.dag_processing.processor import DagFileProcessorProcess
from airflow.exceptions import SerializedDagNotFound
from airflow.executors.executor_loader import UNPICKLEABLE_EXECUTORS
from airflow.jobs.base_job import BaseJob
from airflow.models import DAG
from airflow.models.dag import DagModel
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import SimpleTaskInstance, TaskInstanceKey
from airflow.stats import Stats
from airflow.ti_deps.dependencies_states import EXECUTION_STATES
from airflow.utils import timezone
from airflow.utils.callback_requests import CallbackRequest, DagCallbackRequest, TaskCallbackRequest
from airflow.utils.event_scheduler import EventScheduler
from airflow.utils.retries import MAX_DB_RETRIES, retry_db_transaction, run_with_db_retries
from airflow.utils.session import create_session, provide_session
from airflow.utils.sqlalchemy import is_lock_not_available_error, prohibit_commit, skip_locked, with_row_locks
from airflow.utils.state import State
from airflow.utils.types import DagRunType

TI = models.TaskInstance
DR = models.DagRun
DM = models.DagModel


def _is_parent_process():
    """
    Returns True if the current process is the parent process. False if the current process is a child
    process started by multiprocessing.
    """
    return multiprocessing.current_process().name == 'MainProcess'


class SchedulerJob(BaseJob):
    """
    This SchedulerJob runs for a specific time interval and schedules the jobs
    that are ready to run. It figures out the latest runs for each
    task and sees if the dependencies for the next schedules are met.
    If so, it creates appropriate TaskInstances and sends run commands to the
    executor. It does this for each task in each DAG and repeats.

    :param subdir: directory containing Python files with Airflow DAG
        definitions, or a specific path to a file
    :type subdir: str
    :param num_runs: The number of times to run the scheduling loop. If you
        have a large number of DAG files this could complete before each file
        has been parsed. -1 for unlimited times.
    :type num_runs: int
    :param num_times_parse_dags: The number of times to try to parse each DAG file.
        -1 for unlimited times.
    :type num_times_parse_dags: int
    :param processor_poll_interval: The number of seconds to wait between
        polls of running processors
    :type processor_poll_interval: int
    :param do_pickle: once a DAG object is obtained by executing the Python
        file, whether to serialize the DAG object to the DB
    :type do_pickle: bool
    :param log: override the default Logger
    :type log: logging.Logger
    """

    __mapper_args__ = {'polymorphic_identity': 'SchedulerJob'}
    heartrate: int = conf.getint('scheduler', 'SCHEDULER_HEARTBEAT_SEC')

    def __init__(
        self,
        subdir: str = settings.DAGS_FOLDER,
        num_runs: int = conf.getint('scheduler', 'num_runs'),
        num_times_parse_dags: int = -1,
        processor_poll_interval: float = conf.getfloat('scheduler', 'processor_poll_interval'),
        do_pickle: bool = False,
        log: logging.Logger = None,
        *args,
        **kwargs,
    ):
        self.subdir = subdir

        self.num_runs = num_runs
        # In specific tests, we want to stop the parse loop after the _files_ have been parsed a certain
        # number of times. This is only to support testing, and isn't something a user is likely to want to
        # configure -- they'll want num_runs
        self.num_times_parse_dags = num_times_parse_dags
        self._processor_poll_interval = processor_poll_interval

        self.do_pickle = do_pickle
        super().__init__(*args, **kwargs)

        if log:
            self._log = log

        # Check what SQL backend we use
        sql_conn: str = conf.get('core', 'sql_alchemy_conn').lower()
        self.using_sqlite = sql_conn.startswith('sqlite')
        self.using_mysql = sql_conn.startswith('mysql')

        self.max_tis_per_query: int = conf.getint('scheduler', 'max_tis_per_query')
        self.processor_agent: Optional[DagFileProcessorAgent] = None

        self.dagbag = DagBag(dag_folder=self.subdir, read_dags_from_db=True, load_op_links=False)

    def register_signals(self) -> None:
        """Register signals that stop child processes"""
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)
        signal.signal(signal.SIGUSR2, self._debug_dump)

    def _exit_gracefully(self, signum, frame) -> None:
        """Helper method to clean up processor_agent to avoid leaving orphan processes."""
        if not _is_parent_process():
            # Only the parent process should perform the cleanup.
            return

        self.log.info("Exiting gracefully upon receiving signal %s", signum)
        if self.processor_agent:
            self.processor_agent.end()
        sys.exit(os.EX_OK)

    def _debug_dump(self, signum, frame):
        if not _is_parent_process():
            # Only the parent process should perform the debug dump.
            return

        try:
            sig_name = signal.Signals(signum).name
        except Exception:
            sig_name = str(signum)

        self.log.info("%s\n%s received, printing debug\n%s", "-" * 80, sig_name, "-" * 80)

        self.executor.debug_dump()
        self.log.info("-" * 80)

    def is_alive(self, grace_multiplier: Optional[float] = None) -> bool:
        """
        Is this SchedulerJob alive?

        We define alive as in a state of running and a heartbeat within the
        threshold defined in the ``scheduler_health_check_threshold`` config
        setting.

        ``grace_multiplier`` is accepted for compatibility with the parent class.

        :rtype: boolean
        """
        if grace_multiplier is not None:
            # Accept the same behaviour as superclass
            return super().is_alive(grace_multiplier=grace_multiplier)
        scheduler_health_check_threshold: int = conf.getint('scheduler', 'scheduler_health_check_threshold')
        return (
            self.state == State.RUNNING
            and (timezone.utcnow() - self.latest_heartbeat).total_seconds() < scheduler_health_check_threshold
        )

    @provide_session
    def _change_state_for_tis_without_dagrun(
        self, old_states: List[str], new_state: str, session: Session = None
    ) -> None:
        """
        For all DAG IDs in the DagBag, look for task instances in the
        old_states and set them to new_state if the corresponding DagRun
        does not exist or exists but is not in the running state. This
        normally should not happen, but it can if the state of DagRuns are
        changed manually.

        :param old_states: examine TaskInstances in this state
        :type old_states: list[airflow.utils.state.State]
        :param new_state: set TaskInstances to this state
        :type new_state: airflow.utils.state.State
        """
        tis_changed = 0
        query = (
            session.query(models.TaskInstance)
            .outerjoin(models.TaskInstance.dag_run)
            .filter(models.TaskInstance.dag_id.in_(list(self.dagbag.dag_ids)))
            .filter(models.TaskInstance.state.in_(old_states))
            .filter(
                or_(
                    models.DagRun.state != State.RUNNING,
                    models.DagRun.state.is_(None),
                )
            )
        )
        # We need to do this for mysql as well because it can cause deadlocks
        # as discussed in https://issues.apache.org/jira/browse/AIRFLOW-2516
        if self.using_sqlite or self.using_mysql:
            tis_to_change: List[TI] = with_row_locks(
                query, of=TI, session=session, **skip_locked(session=session)
            ).all()
            for ti in tis_to_change:
                ti.set_state(new_state, session=session)
                tis_changed += 1
        else:
            subq = query.subquery()
            current_time = timezone.utcnow()
            ti_prop_update = {
                models.TaskInstance.state: new_state,
                models.TaskInstance.start_date: current_time,
            }

            # Only add end_date and duration if the new_state is 'success', 'failed' or 'skipped'
            if new_state in State.finished:
                ti_prop_update.update(
                    {
                        models.TaskInstance.end_date: current_time,
                        models.TaskInstance.duration: 0,
                    }
                )

            tis_changed = (
                session.query(models.TaskInstance)
                .filter(
                    models.TaskInstance.dag_id == subq.c.dag_id,
                    models.TaskInstance.task_id == subq.c.task_id,
                    models.TaskInstance.execution_date == subq.c.execution_date,
                )
                .update(ti_prop_update, synchronize_session=False)
            )

        if tis_changed > 0:
            session.flush()
            self.log.warning(
                "Set %s task instances to state=%s as their associated DagRun was not in RUNNING state",
                tis_changed,
                new_state,
            )
            Stats.gauge('scheduler.tasks.without_dagrun', tis_changed)

    @provide_session
    def __get_concurrency_maps(
        self, states: List[str], session: Session = None
    ) -> Tuple[DefaultDict[str, int], DefaultDict[Tuple[str, str], int]]:
        """
        Get the concurrency maps.

        :param states: List of states to query for
        :type states: list[airflow.utils.state.State]
        :return: A map from (dag_id, task_id) to # of task instances and
         a map from (dag_id, task_id) to # of task instances in the given state list
        :rtype: tuple[dict[str, int], dict[tuple[str, str], int]]
        """
        ti_concurrency_query: List[Tuple[str, str, int]] = (
            session.query(TI.task_id, TI.dag_id, func.count('*'))
            .filter(TI.state.in_(states))
            .group_by(TI.task_id, TI.dag_id)
        ).all()
        dag_map: DefaultDict[str, int] = defaultdict(int)
        task_map: DefaultDict[Tuple[str, str], int] = defaultdict(int)
        for result in ti_concurrency_query:
            task_id, dag_id, count = result
            dag_map[dag_id] += count
            task_map[(dag_id, task_id)] = count
        return dag_map, task_map

    @provide_session
    def _executable_task_instances_to_queued(self, max_tis: int, session: Session = None) -> List[TI]:
        """
        Finds TIs that are ready for execution with respect to pool limits,
        dag max_active_tasks, executor state, and priority.

        :param max_tis: Maximum number of TIs to queue in this loop.
        :type max_tis: int
        :return: list[airflow.models.TaskInstance]
        """
        executable_tis: List[TI] = []

        # Get the pool settings. We get a lock on the pool rows, treating this as a "critical section"
        # Throws an exception if lock cannot be obtained, rather than blocking
        pools = models.Pool.slots_stats(lock_rows=True, session=session)

        # If the pools are full, there is no point doing anything!
        # If _somehow_ the pool is overfull, don't let the limit go negative - it breaks SQL
        pool_slots_free = max(0, sum(pool['open'] for pool in pools.values()))

        if pool_slots_free == 0:
            self.log.debug("All pools are full!")
            return executable_tis

        max_tis = min(max_tis, pool_slots_free)

        # Get all task instances associated with scheduled
        # DagRuns which are not backfilled, in the given states,
        # and the dag is not paused
        query = (
            session.query(TI)
            .outerjoin(TI.dag_run)
            .filter(or_(DR.run_id.is_(None), DR.run_type != DagRunType.BACKFILL_JOB))
            .join(TI.dag_model)
            .filter(not_(DM.is_paused))
            .filter(TI.state == State.SCHEDULED)
            .options(selectinload('dag_model'))
            .order_by(-TI.priority_weight, TI.execution_date)
        )
        starved_pools = [pool_name for pool_name, stats in pools.items() if stats['open'] <= 0]
        if starved_pools:
            query = query.filter(not_(TI.pool.in_(starved_pools)))

        query = query.limit(max_tis)

        task_instances_to_examine: List[TI] = with_row_locks(
            query,
            of=TI,
            session=session,
            **skip_locked(session=session),
        ).all()
        # TODO[HA]: This was wrong before anyway, as it only looked at a sub-set of dags, not everything.
        # Stats.gauge('scheduler.tasks.pending', len(task_instances_to_examine))

        if len(task_instances_to_examine) == 0:
            self.log.debug("No tasks to consider for execution.")
            return executable_tis

        # Put one task instance on each line
        task_instance_str = "\n\t".join(repr(x) for x in task_instances_to_examine)
        self.log.info("%s tasks up for execution:\n\t%s", len(task_instances_to_examine), task_instance_str)

        pool_to_task_instances: DefaultDict[str, List[models.Pool]] = defaultdict(list)
        for task_instance in task_instances_to_examine:
            pool_to_task_instances[task_instance.pool].append(task_instance)

        # dag_id to # of running tasks and (dag_id, task_id) to # of running tasks.
        dag_max_active_tasks_map: DefaultDict[str, int]
        task_concurrency_map: DefaultDict[Tuple[str, str], int]
        dag_max_active_tasks_map, task_concurrency_map = self.__get_concurrency_maps(
            states=list(EXECUTION_STATES), session=session
        )

        num_tasks_in_executor = 0
        # Number of tasks that cannot be scheduled because of no open slot in pool
        num_starving_tasks_total = 0

        # Go through each pool, and queue up a task for execution if there are
        # any open slots in the pool.

        for pool, task_instances in pool_to_task_instances.items():
            pool_name = pool
            if pool not in pools:
                self.log.warning("Tasks using non-existent pool '%s' will not be scheduled", pool)
                continue

            open_slots = pools[pool]["open"]

            num_ready = len(task_instances)
            self.log.info(
                "Figuring out tasks to run in Pool(name=%s) with %s open slots "
                "and %s task instances ready to be queued",
                pool,
                open_slots,
                num_ready,
            )

            priority_sorted_task_instances = sorted(
                task_instances, key=lambda ti: (-ti.priority_weight, ti.execution_date)
            )

            num_starving_tasks = 0
            for current_index, task_instance in enumerate(priority_sorted_task_instances):
                if open_slots <= 0:
                    self.log.info("Not scheduling since there are %s open slots in pool %s", open_slots, pool)
                    # Can't schedule any more since there are no more open slots.
                    num_unhandled = len(priority_sorted_task_instances) - current_index
                    num_starving_tasks += num_unhandled
                    num_starving_tasks_total += num_unhandled
                    break

                # Check to make sure that the task max_active_tasks of the DAG hasn't been
                # reached.
                dag_id = task_instance.dag_id

                current_max_active_tasks_per_dag = dag_max_active_tasks_map[dag_id]
                max_active_tasks_per_dag_limit = task_instance.dag_model.max_active_tasks
                self.log.info(
                    "DAG %s has %s/%s running and queued tasks",
                    dag_id,
                    current_max_active_tasks_per_dag,
                    max_active_tasks_per_dag_limit,
                )
                if current_max_active_tasks_per_dag >= max_active_tasks_per_dag_limit:
                    self.log.info(
                        "Not executing %s since the number of tasks running or queued "
                        "from DAG %s is >= to the DAG's max_active_tasks limit of %s",
                        task_instance,
                        dag_id,
                        max_active_tasks_per_dag_limit,
                    )
                    continue

                task_concurrency_limit: Optional[int] = None
                if task_instance.dag_model.has_task_concurrency_limits:
                    # Many dags don't have a task_concurrency, so where we can avoid loading the full
                    # serialized DAG the better.
                    serialized_dag = self.dagbag.get_dag(dag_id, session=session)
                    if serialized_dag.has_task(task_instance.task_id):
                        task_concurrency_limit = serialized_dag.get_task(
                            task_instance.task_id
                        ).task_concurrency

                    if task_concurrency_limit is not None:
                        current_task_concurrency = task_concurrency_map[
                            (task_instance.dag_id, task_instance.task_id)
                        ]

                        if current_task_concurrency >= task_concurrency_limit:
                            self.log.info(
                                "Not executing %s since the task concurrency for"
                                " this task has been reached.",
                                task_instance,
                            )
                            continue

                if task_instance.pool_slots > open_slots:
                    self.log.info(
                        "Not executing %s since it requires %s slots "
                        "but there are %s open slots in the pool %s.",
                        task_instance,
                        task_instance.pool_slots,
                        open_slots,
                        pool,
                    )
                    num_starving_tasks += 1
                    num_starving_tasks_total += 1
                    # Though we can execute tasks with lower priority if there's enough room
                    continue

                executable_tis.append(task_instance)
                open_slots -= task_instance.pool_slots
                dag_max_active_tasks_map[dag_id] += 1
                task_concurrency_map[(task_instance.dag_id, task_instance.task_id)] += 1

            Stats.gauge(f'pool.starving_tasks.{pool_name}', num_starving_tasks)

        Stats.gauge('scheduler.tasks.starving', num_starving_tasks_total)
        Stats.gauge('scheduler.tasks.running', num_tasks_in_executor)
        Stats.gauge('scheduler.tasks.executable', len(executable_tis))

        task_instance_str = "\n\t".join(repr(x) for x in executable_tis)
        self.log.info("Setting the following tasks to queued state:\n\t%s", task_instance_str)
        if len(executable_tis) > 0:
            # set TIs to queued state
            filter_for_tis = TI.filter_for_tis(executable_tis)
            session.query(TI).filter(filter_for_tis).update(
                # TODO[ha]: should we use func.now()? How does that work with DB timezone
                # on mysql when it's not UTC?
                {TI.state: State.QUEUED, TI.queued_dttm: timezone.utcnow(), TI.queued_by_job_id: self.id},
                synchronize_session=False,
            )

        for ti in executable_tis:
            make_transient(ti)
        return executable_tis

    def _enqueue_task_instances_with_queued_state(self, task_instances: List[TI]) -> None:
        """
        Takes task_instances, which should have been set to queued, and enqueues them
        with the executor.

        :param task_instances: TaskInstances to enqueue
        :type task_instances: list[TaskInstance]
        """
        # actually enqueue them
        for ti in task_instances:
            command = TI.generate_command(
                ti.dag_id,
                ti.task_id,
                ti.execution_date,
                local=True,
                mark_success=False,
                ignore_all_deps=False,
                ignore_depends_on_past=False,
                ignore_task_deps=False,
                ignore_ti_state=False,
                pool=ti.pool,
                file_path=ti.dag_model.fileloc,
                pickle_id=ti.dag_model.pickle_id,
            )

            priority = ti.priority_weight
            queue = ti.queue
            self.log.info("Sending %s to executor with priority %s and queue %s", ti.key, priority, queue)

            self.executor.queue_command(
                ti,
                command,
                priority=priority,
                queue=queue,
            )

    def _critical_section_execute_task_instances(self, session: Session) -> int:
        """
        Attempts to execute TaskInstances that should be executed by the scheduler.

        There are three steps:
        1. Pick TIs by priority with the constraint that they are in the expected states
        and that we do exceed max_active_runs or pool limits.
        2. Change the state for the TIs above atomically.
        3. Enqueue the TIs in the executor.

        HA note: This function is a "critical section" meaning that only a single executor process can execute
        this function at the same time. This is achieved by doing ``SELECT ... from pool FOR UPDATE``. For DBs
        that support NOWAIT, a "blocked" scheduler will skip this and continue on with other tasks (creating
        new DAG runs, progressing TIs from None to SCHEDULED etc.); DBs that don't support this (such as
        MariaDB or MySQL 5.x) the other schedulers will wait for the lock before continuing.

        :param session:
        :type session: sqlalchemy.orm.Session
        :return: Number of task instance with state changed.
        """
        if self.max_tis_per_query == 0:
            max_tis = self.executor.slots_available
        else:
            max_tis = min(self.max_tis_per_query, self.executor.slots_available)
        queued_tis = self._executable_task_instances_to_queued(max_tis, session=session)

        self._enqueue_task_instances_with_queued_state(queued_tis)
        return len(queued_tis)

    @provide_session
    def _process_executor_events(self, session: Session = None) -> int:
        """Respond to executor events."""
        if not self.processor_agent:
            raise ValueError("Processor agent is not started.")
        ti_primary_key_to_try_number_map: Dict[Tuple[str, str, datetime.datetime], int] = {}
        event_buffer = self.executor.get_event_buffer()
        tis_with_right_state: List[TaskInstanceKey] = []

        # Report execution
        for ti_key, value in event_buffer.items():
            state: str
            state, _ = value
            # We create map (dag_id, task_id, execution_date) -> in-memory try_number
            ti_primary_key_to_try_number_map[ti_key.primary] = ti_key.try_number

            self.log.info(
                "Executor reports execution of %s.%s execution_date=%s "
                "exited with status %s for try_number %s",
                ti_key.dag_id,
                ti_key.task_id,
                ti_key.execution_date,
                state,
                ti_key.try_number,
            )
            if state in (State.FAILED, State.SUCCESS, State.QUEUED):
                tis_with_right_state.append(ti_key)

        # Return if no finished tasks
        if not tis_with_right_state:
            return len(event_buffer)

        # Check state of finished tasks
        filter_for_tis = TI.filter_for_tis(tis_with_right_state)
        tis: List[TI] = session.query(TI).filter(filter_for_tis).options(selectinload('dag_model')).all()
        for ti in tis:
            try_number = ti_primary_key_to_try_number_map[ti.key.primary]
            buffer_key = ti.key.with_try_number(try_number)
            state, info = event_buffer.pop(buffer_key)

            # TODO: should we fail RUNNING as well, as we do in Backfills?
            if state == State.QUEUED:
                ti.external_executor_id = info
                self.log.info("Setting external_id for %s to %s", ti, info)
                continue

            if ti.try_number == buffer_key.try_number and ti.state == State.QUEUED:
                Stats.incr('scheduler.tasks.killed_externally')
                msg = (
                    "Executor reports task instance %s finished (%s) although the "
                    "task says its %s. (Info: %s) Was the task killed externally?"
                )
                self.log.error(msg, ti, state, ti.state, info)

                request = TaskCallbackRequest(
                    full_filepath=ti.dag_model.fileloc,
                    simple_task_instance=SimpleTaskInstance(ti),
                    msg=msg % (ti, state, ti.state, info),
                )
                self.log.info('Setting task instance %s state to %s as reported by executor', ti, state)
                ti.set_state(state)
                self.processor_agent.send_callback_to_execute(request)

        return len(event_buffer)

    def _execute(self) -> None:
        self.log.info("Starting the scheduler")

        # DAGs can be pickled for easier remote execution by some executors
        pickle_dags = self.do_pickle and self.executor_class not in UNPICKLEABLE_EXECUTORS

        self.log.info("Processing each file at most %s times", self.num_times_parse_dags)

        # When using sqlite, we do not use async_mode
        # so the scheduler job and DAG parser don't access the DB at the same time.
        async_mode = not self.using_sqlite

        processor_timeout_seconds: int = conf.getint('core', 'dag_file_processor_timeout')
        processor_timeout = timedelta(seconds=processor_timeout_seconds)
        self.processor_agent = DagFileProcessorAgent(
            dag_directory=self.subdir,
            max_runs=self.num_times_parse_dags,
            processor_factory=type(self)._create_dag_file_processor,
            processor_timeout=processor_timeout,
            dag_ids=[],
            pickle_dags=pickle_dags,
            async_mode=async_mode,
        )

        try:
            self.executor.job_id = self.id
            self.executor.start()

            self.register_signals()

            self.processor_agent.start()

            execute_start_time = timezone.utcnow()

            self._run_scheduler_loop()

            # Stop any processors
            self.processor_agent.terminate()

            # Verify that all files were processed, and if so, deactivate DAGs that
            # haven't been touched by the scheduler as they likely have been
            # deleted.
            if self.processor_agent.all_files_processed:
                self.log.info(
                    "Deactivating DAGs that haven't been touched since %s", execute_start_time.isoformat()
                )
                models.DAG.deactivate_stale_dags(execute_start_time)

            settings.Session.remove()  # type: ignore
        except Exception:
            self.log.exception("Exception when executing SchedulerJob._run_scheduler_loop")
            raise
        finally:
            try:
                self.executor.end()
            except Exception:
                self.log.exception("Exception when executing Executor.end")
            try:
                self.processor_agent.end()
            except Exception:
                self.log.exception("Exception when executing DagFileProcessorAgent.end")
            self.log.info("Exited execute loop")

    @staticmethod
    def _create_dag_file_processor(
        file_path: str,
        callback_requests: List[CallbackRequest],
        dag_ids: Optional[List[str]],
        pickle_dags: bool,
    ) -> DagFileProcessorProcess:
        """Creates DagFileProcessorProcess instance."""
        return DagFileProcessorProcess(
            file_path=file_path, pickle_dags=pickle_dags, dag_ids=dag_ids, callback_requests=callback_requests
        )

    def _run_scheduler_loop(self) -> None:
        """
        The actual scheduler loop. The main steps in the loop are:
            #. Harvest DAG parsing results through DagFileProcessorAgent
            #. Find and queue executable tasks
                #. Change task instance state in DB
                #. Queue tasks in executor
            #. Heartbeat executor
                #. Execute queued tasks in executor asynchronously
                #. Sync on the states of running tasks

        Following is a graphic representation of these steps.

        .. image:: ../docs/apache-airflow/img/scheduler_loop.jpg

        :rtype: None
        """
        if not self.processor_agent:
            raise ValueError("Processor agent is not started.")
        is_unit_test: bool = conf.getboolean('core', 'unit_test_mode')

        timers = EventScheduler()

        # Check on start up, then every configured interval
        self.adopt_or_reset_orphaned_tasks()

        timers.call_regular_interval(
            conf.getfloat('scheduler', 'orphaned_tasks_check_interval', fallback=300.0),
            self.adopt_or_reset_orphaned_tasks,
        )

        timers.call_regular_interval(
            conf.getfloat('scheduler', 'pool_metrics_interval', fallback=5.0),
            self._emit_pool_metrics,
        )

        timers.call_regular_interval(
            conf.getfloat('scheduler', 'clean_tis_without_dagrun_interval', fallback=15.0),
            self._clean_tis_without_dagrun,
        )

        for loop_count in itertools.count(start=1):
            with Stats.timer() as timer:

                if self.using_sqlite:
                    self.processor_agent.run_single_parsing_loop()
                    # For the sqlite case w/ 1 thread, wait until the processor
                    # is finished to avoid concurrent access to the DB.
                    self.log.debug("Waiting for processors to finish since we're using sqlite")
                    self.processor_agent.wait_until_finished()

                with create_session() as session:
                    num_queued_tis = self._do_scheduling(session)

                    self.executor.heartbeat()
                    session.expunge_all()
                    num_finished_events = self._process_executor_events(session=session)

                self.processor_agent.heartbeat()

                # Heartbeat the scheduler periodically
                self.heartbeat(only_if_necessary=True)

                # Run any pending timed events
                next_event = timers.run(blocking=False)
                self.log.debug("Next timed event is in %f", next_event)

            self.log.debug("Ran scheduling loop in %.2f seconds", timer.duration)

            if not is_unit_test and not num_queued_tis and not num_finished_events:
                # If the scheduler is doing things, don't sleep. This means when there is work to do, the
                # scheduler will run "as quick as possible", but when it's stopped, it can sleep, dropping CPU
                # usage when "idle"
                time.sleep(min(self._processor_poll_interval, next_event))

            if loop_count >= self.num_runs > 0:
                self.log.info(
                    "Exiting scheduler loop as requested number of runs (%d - got to %d) has been reached",
                    self.num_runs,
                    loop_count,
                )
                break
            if self.processor_agent.done:
                self.log.info(
                    "Exiting scheduler loop as requested DAG parse count (%d) has been reached after %d"
                    " scheduler loops",
                    self.num_times_parse_dags,
                    loop_count,
                )
                break

    @provide_session
    def _clean_tis_without_dagrun(self, session):
        with prohibit_commit(session) as guard:
            try:
                self._change_state_for_tis_without_dagrun(
                    old_states=[State.UP_FOR_RETRY], new_state=State.FAILED, session=session
                )

                self._change_state_for_tis_without_dagrun(
                    old_states=[State.QUEUED, State.SCHEDULED, State.UP_FOR_RESCHEDULE, State.SENSING],
                    new_state=State.NONE,
                    session=session,
                )

                guard.commit()
            except OperationalError as e:
                if is_lock_not_available_error(error=e):
                    self.log.debug("Lock held by another Scheduler")
                    session.rollback()
                else:
                    raise
            guard.commit()

    def _do_scheduling(self, session) -> int:
        """
        This function is where the main scheduling decisions take places. It:

        - Creates any necessary DAG runs by examining the next_dagrun_create_after column of DagModel

          Since creating Dag Runs is a relatively time consuming process, we select only 10 dags by default
          (configurable via ``scheduler.max_dagruns_to_create_per_loop`` setting) - putting this higher will
          mean one scheduler could spend a chunk of time creating dag runs, and not ever get around to
          scheduling tasks.

        - Finds the "next n oldest" running DAG Runs to examine for scheduling (n=20 by default, configurable
          via ``scheduler.max_dagruns_per_loop_to_schedule`` config setting) and tries to progress state (TIs
          to SCHEDULED, or DagRuns to SUCCESS/FAILURE etc)

          By "next oldest", we mean hasn't been examined/scheduled in the most time.

          The reason we don't select all dagruns at once because the rows are selected with row locks, meaning
          that only one scheduler can "process them", even it is waiting behind other dags. Increasing this
          limit will allow more throughput for smaller DAGs but will likely slow down throughput for larger
          (>500 tasks.) DAGs

        - Then, via a Critical Section (locking the rows of the Pool model) we queue tasks, and then send them
          to the executor.

          See docs of _critical_section_execute_task_instances for more.

        :return: Number of TIs enqueued in this iteration
        :rtype: int
        """
        # Put a check in place to make sure we don't commit unexpectedly
        with prohibit_commit(session) as guard:

            if settings.USE_JOB_SCHEDULE:
                self._create_dagruns_for_dags(guard, session)

            dag_runs = self._get_next_dagruns_to_examine(session)
            # Bulk fetch the currently active dag runs for the dags we are
            # examining, rather than making one query per DagRun

            # TODO: This query is probably horribly inefficient (though there is an
            # index on (dag_id,state)). It is to deal with the case when a user
            # clears more than max_active_runs older tasks -- we don't want the
            # scheduler to suddenly go and start running tasks from all of the
            # runs. (AIRFLOW-137/GH #1442)
            #
            # The longer term fix would be to have `clear` do this, and put DagRuns
            # in to the queued state, then take DRs out of queued before creating
            # any new ones

            # Build up a set of execution_dates that are "active" for a given
            # dag_id -- only tasks from those runs will be scheduled.
            active_runs_by_dag_id = defaultdict(set)

            query = (
                session.query(
                    TI.dag_id,
                    TI.execution_date,
                )
                .filter(
                    TI.dag_id.in_(list({dag_run.dag_id for dag_run in dag_runs})),
                    TI.state.notin_(list(State.finished) + [State.REMOVED]),
                )
                .group_by(TI.dag_id, TI.execution_date)
            )

            for dag_id, execution_date in query:
                active_runs_by_dag_id[dag_id].add(execution_date)

            for dag_run in dag_runs:
                # Use try_except to not stop the Scheduler when a Serialized DAG is not found
                # This takes care of Dynamic DAGs especially
                # SerializedDagNotFound should not happen here in the same loop because the DagRun would
                # not be created in self._create_dag_runs if Serialized DAG does not exist
                # But this would take care of the scenario when the Scheduler is restarted after DagRun is
                # created and the DAG is deleted / renamed
                try:
                    self._schedule_dag_run(dag_run, active_runs_by_dag_id.get(dag_run.dag_id, set()), session)
                except SerializedDagNotFound:
                    self.log.exception("DAG '%s' not found in serialized_dag table", dag_run.dag_id)
                    continue

            guard.commit()

            # Without this, the session has an invalid view of the DB
            session.expunge_all()
            # END: schedule TIs

            try:
                if self.executor.slots_available <= 0:
                    # We know we can't do anything here, so don't even try!
                    self.log.debug("Executor full, skipping critical section")
                    return 0

                timer = Stats.timer('scheduler.critical_section_duration')
                timer.start()

                # Find anything TIs in state SCHEDULED, try to QUEUE it (send it to the executor)
                num_queued_tis = self._critical_section_execute_task_instances(session=session)

                # Make sure we only sent this metric if we obtained the lock, otherwise we'll skew the
                # metric, way down
                timer.stop(send=True)
            except OperationalError as e:
                timer.stop(send=False)

                if is_lock_not_available_error(error=e):
                    self.log.debug("Critical section lock held by another Scheduler")
                    Stats.incr('scheduler.critical_section_busy')
                    session.rollback()
                    return 0
                raise

            guard.commit()
            return num_queued_tis

    @retry_db_transaction
    def _get_next_dagruns_to_examine(self, session):
        """Get Next DagRuns to Examine with retries"""
        return DagRun.next_dagruns_to_examine(session)

    @retry_db_transaction
    def _create_dagruns_for_dags(self, guard, session):
        """Find Dag Models needing DagRuns and Create Dag Runs with retries in case of OperationalError"""
        query = DagModel.dags_needing_dagruns(session)
        self._create_dag_runs(query.all(), session)

        # commit the session - Release the write lock on DagModel table.
        guard.commit()
        # END: create dagruns

    def _create_dag_runs(self, dag_models: Iterable[DagModel], session: Session) -> None:
        """
        Unconditionally create a DAG run for the given DAG, and update the dag_model's fields to control
        if/when the next DAGRun should be created
        """
        # Bulk Fetch DagRuns with dag_id and execution_date same
        # as DagModel.dag_id and DagModel.next_dagrun
        # This list is used to verify if the DagRun already exist so that we don't attempt to create
        # duplicate dag runs

        if session.bind.dialect.name == 'mssql':
            active_dagruns_filter = or_(
                *(
                    and_(
                        DagRun.dag_id == dm.dag_id,
                        DagRun.execution_date == dm.next_dagrun,
                    )
                    for dm in dag_models
                )
            )
        else:
            active_dagruns_filter = tuple_(DagRun.dag_id, DagRun.execution_date).in_(
                [(dm.dag_id, dm.next_dagrun) for dm in dag_models]
            )

        active_dagruns = (
            session.query(DagRun.dag_id, DagRun.execution_date).filter(active_dagruns_filter).all()
        )

        for dag_model in dag_models:
            try:
                dag = self.dagbag.get_dag(dag_model.dag_id, session=session)
            except SerializedDagNotFound:
                self.log.exception("DAG '%s' not found in serialized_dag table", dag_model.dag_id)
                continue

            dag_hash = self.dagbag.dags_hash.get(dag.dag_id)
            # Explicitly check if the DagRun already exists. This is an edge case
            # where a Dag Run is created but `DagModel.next_dagrun` and `DagModel.next_dagrun_create_after`
            # are not updated.
            # We opted to check DagRun existence instead
            # of catching an Integrity error and rolling back the session i.e
            # we need to run self._update_dag_next_dagruns if the Dag Run already exists or if we
            # create a new one. This is so that in the next Scheduling loop we try to create new runs
            # instead of falling in a loop of Integrity Error.
            if (dag.dag_id, dag_model.next_dagrun) not in active_dagruns:
                run = dag.create_dagrun(
                    run_type=DagRunType.SCHEDULED,
                    execution_date=dag_model.next_dagrun,
                    start_date=timezone.utcnow(),
                    state=State.RUNNING,
                    external_trigger=False,
                    session=session,
                    dag_hash=dag_hash,
                    creating_job_id=self.id,
                )

                expected_start_date = dag.following_schedule(run.execution_date)
                if expected_start_date:
                    schedule_delay = run.start_date - expected_start_date
                    Stats.timing(
                        f'dagrun.schedule_delay.{dag.dag_id}',
                        schedule_delay,
                    )

        self._update_dag_next_dagruns(dag_models, session)

        # TODO[HA]: Should we do a session.flush() so we don't have to keep lots of state/object in
        # memory for larger dags? or expunge_all()

    def _update_dag_next_dagruns(self, dag_models: Iterable[DagModel], session: Session) -> None:
        """
        Bulk update the next_dagrun and next_dagrun_create_after for all the dags.

        We batch the select queries to get info about all the dags at once
        """
        # Check max_active_runs, to see if we are _now_ at the limit for any of
        # these dag? (we've just created a DagRun for them after all)
        active_runs_of_dags = dict(
            session.query(DagRun.dag_id, func.count('*'))
            .filter(
                DagRun.dag_id.in_([o.dag_id for o in dag_models]),
                DagRun.state == State.RUNNING,
                DagRun.external_trigger == expression.false(),
            )
            .group_by(DagRun.dag_id)
            .all()
        )

        for dag_model in dag_models:
            # Get the DAG in a try_except to not stop the Scheduler when a Serialized DAG is not found
            # This takes care of Dynamic DAGs especially
            try:
                dag = self.dagbag.get_dag(dag_model.dag_id, session=session)
            except SerializedDagNotFound:
                self.log.exception("DAG '%s' not found in serialized_dag table", dag_model.dag_id)
                continue
            active_runs_of_dag = active_runs_of_dags.get(dag.dag_id, 0)
            if dag.max_active_runs and active_runs_of_dag >= dag.max_active_runs:
                self.log.info(
                    "DAG %s is at (or above) max_active_runs (%d of %d), not creating any more runs",
                    dag.dag_id,
                    active_runs_of_dag,
                    dag.max_active_runs,
                )
                dag_model.next_dagrun_create_after = None
            else:
                dag_model.next_dagrun, dag_model.next_dagrun_create_after = dag.next_dagrun_info(
                    dag_model.next_dagrun
                )

    def _schedule_dag_run(
        self,
        dag_run: DagRun,
        currently_active_runs: Set[datetime.datetime],
        session: Session,
    ) -> int:
        """
        Make scheduling decisions about an individual dag run

        ``currently_active_runs`` is passed in so that a batch query can be
        used to ask this for all dag runs in the batch, to avoid an n+1 query.

        :param dag_run: The DagRun to schedule
        :param currently_active_runs: Number of currently active runs of this DAG
        :return: Number of tasks scheduled
        """
        dag = dag_run.dag = self.dagbag.get_dag(dag_run.dag_id, session=session)

        if not dag:
            self.log.error("Couldn't find dag %s in DagBag/DB!", dag_run.dag_id)
            return 0

        if (
            dag_run.start_date
            and dag.dagrun_timeout
            and dag_run.start_date < timezone.utcnow() - dag.dagrun_timeout
        ):
            dag_run.set_state(State.FAILED)
            unfinished_task_instances = (
                session.query(TI)
                .filter(TI.dag_id == dag_run.dag_id)
                .filter(TI.execution_date == dag_run.execution_date)
                .filter(TI.state.in_(State.unfinished))
            )
            for task_instance in unfinished_task_instances:
                task_instance.state = State.SKIPPED
                session.merge(task_instance)
            session.flush()
            self.log.info("Run %s of %s has timed-out", dag_run.run_id, dag_run.dag_id)

            # Work out if we should allow creating a new DagRun now?
            self._update_dag_next_dagruns([session.query(DagModel).get(dag_run.dag_id)], session)

            callback_to_execute = DagCallbackRequest(
                full_filepath=dag.fileloc,
                dag_id=dag.dag_id,
                execution_date=dag_run.execution_date,
                is_failure_callback=True,
                msg='timed_out',
            )

            # Send SLA & DAG Success/Failure Callbacks to be executed
            self._send_dag_callbacks_to_processor(dag_run, callback_to_execute)

            return 0

        if dag_run.execution_date > timezone.utcnow() and not dag.allow_future_exec_dates:
            self.log.error("Execution date is in future: %s", dag_run.execution_date)
            return 0

        if dag.max_active_runs:
            if (
                len(currently_active_runs) >= dag.max_active_runs
                and dag_run.execution_date not in currently_active_runs
            ):
                self.log.info(
                    "DAG %s already has %d active runs, not queuing any tasks for run %s",
                    dag.dag_id,
                    len(currently_active_runs),
                    dag_run.execution_date,
                )
                return 0

        self._verify_integrity_if_dag_changed(dag_run=dag_run, session=session)
        # TODO[HA]: Rename update_state -> schedule_dag_run, ?? something else?
        schedulable_tis, callback_to_run = dag_run.update_state(session=session, execute_callbacks=False)

        self._send_dag_callbacks_to_processor(dag_run, callback_to_run)

        # This will do one query per dag run. We "could" build up a complex
        # query to update all the TIs across all the execution dates and dag
        # IDs in a single query, but it turns out that can be _very very slow_
        # see #11147/commit ee90807ac for more details
        return dag_run.schedule_tis(schedulable_tis, session)

    @provide_session
    def _verify_integrity_if_dag_changed(self, dag_run: DagRun, session=None):
        """Only run DagRun.verify integrity if Serialized DAG has changed since it is slow"""
        latest_version = SerializedDagModel.get_latest_version_hash(dag_run.dag_id, session=session)
        if dag_run.dag_hash == latest_version:
            self.log.debug("DAG %s not changed structure, skipping dagrun.verify_integrity", dag_run.dag_id)
            return

        dag_run.dag_hash = latest_version

        # Refresh the DAG
        dag_run.dag = self.dagbag.get_dag(dag_id=dag_run.dag_id, session=session)

        # Verify integrity also takes care of session.flush
        dag_run.verify_integrity(session=session)

    def _send_dag_callbacks_to_processor(
        self, dag_run: DagRun, callback: Optional[DagCallbackRequest] = None
    ):
        if not self.processor_agent:
            raise ValueError("Processor agent is not started.")

        dag = dag_run.get_dag()
        self._send_sla_callbacks_to_processor(dag)
        if callback:
            self.processor_agent.send_callback_to_execute(callback)

    def _send_sla_callbacks_to_processor(self, dag: DAG):
        """Sends SLA Callbacks to DagFileProcessor if tasks have SLAs set and check_slas=True"""
        if not settings.CHECK_SLAS:
            return

        if not any(isinstance(ti.sla, timedelta) for ti in dag.tasks):
            self.log.debug("Skipping SLA check for %s because no tasks in DAG have SLAs", dag)
            return

        if not self.processor_agent:
            raise ValueError("Processor agent is not started.")

        self.processor_agent.send_sla_callback_request_to_execute(
            full_filepath=dag.fileloc, dag_id=dag.dag_id
        )

    @provide_session
    def _emit_pool_metrics(self, session: Session = None) -> None:
        pools = models.Pool.slots_stats(session=session)
        for pool_name, slot_stats in pools.items():
            Stats.gauge(f'pool.open_slots.{pool_name}', slot_stats["open"])
            Stats.gauge(f'pool.queued_slots.{pool_name}', slot_stats[State.QUEUED])  # type: ignore
            Stats.gauge(f'pool.running_slots.{pool_name}', slot_stats[State.RUNNING])  # type: ignore

    @provide_session
    def heartbeat_callback(self, session: Session = None) -> None:
        Stats.incr('scheduler_heartbeat', 1, 1)

    @provide_session
    def adopt_or_reset_orphaned_tasks(self, session: Session = None):
        """
        Reset any TaskInstance still in QUEUED or SCHEDULED states that were
        enqueued by a SchedulerJob that is no longer running.

        :return: the number of TIs reset
        :rtype: int
        """
        self.log.info("Resetting orphaned tasks for active dag runs")
        timeout = conf.getint('scheduler', 'scheduler_health_check_threshold')

        for attempt in run_with_db_retries(logger=self.log):
            with attempt:
                self.log.debug(
                    "Running SchedulerJob.adopt_or_reset_orphaned_tasks with retries. Try %d of %d",
                    attempt.retry_state.attempt_number,
                    MAX_DB_RETRIES,
                )
                self.log.debug("Calling SchedulerJob.adopt_or_reset_orphaned_tasks method")
                try:
                    num_failed = (
                        session.query(SchedulerJob)
                        .filter(
                            SchedulerJob.state == State.RUNNING,
                            SchedulerJob.latest_heartbeat < (timezone.utcnow() - timedelta(seconds=timeout)),
                        )
                        .update({"state": State.FAILED})
                    )

                    if num_failed:
                        self.log.info("Marked %d SchedulerJob instances as failed", num_failed)
                        Stats.incr(self.__class__.__name__.lower() + '_end', num_failed)

                    resettable_states = [State.QUEUED, State.RUNNING]
                    query = (
                        session.query(TI)
                        .filter(TI.state.in_(resettable_states))
                        # outerjoin is because we didn't use to have queued_by_job
                        # set, so we need to pick up anything pre upgrade. This (and the
                        # "or queued_by_job_id IS NONE") can go as soon as scheduler HA is
                        # released.
                        .outerjoin(TI.queued_by_job)
                        .filter(or_(TI.queued_by_job_id.is_(None), SchedulerJob.state != State.RUNNING))
                        .join(TI.dag_run)
                        .filter(
                            DagRun.run_type != DagRunType.BACKFILL_JOB,
                            DagRun.state == State.RUNNING,
                        )
                        .options(load_only(TI.dag_id, TI.task_id, TI.execution_date))
                    )

                    # Lock these rows, so that another scheduler can't try and adopt these too
                    tis_to_reset_or_adopt = with_row_locks(
                        query, of=TI, session=session, **skip_locked(session=session)
                    ).all()
                    to_reset = self.executor.try_adopt_task_instances(tis_to_reset_or_adopt)

                    reset_tis_message = []
                    for ti in to_reset:
                        reset_tis_message.append(repr(ti))
                        ti.state = State.NONE
                        ti.queued_by_job_id = None

                    for ti in set(tis_to_reset_or_adopt) - set(to_reset):
                        ti.queued_by_job_id = self.id

                    Stats.incr('scheduler.orphaned_tasks.cleared', len(to_reset))
                    Stats.incr('scheduler.orphaned_tasks.adopted', len(tis_to_reset_or_adopt) - len(to_reset))

                    if to_reset:
                        task_instance_str = '\n\t'.join(reset_tis_message)
                        self.log.info(
                            "Reset the following %s orphaned TaskInstances:\n\t%s",
                            len(to_reset),
                            task_instance_str,
                        )

                    # Issue SQL/finish "Unit of Work", but let @provide_session
                    # commit (or if passed a session, let caller decide when to commit
                    session.flush()
                except OperationalError:
                    session.rollback()
                    raise

        return len(to_reset)
