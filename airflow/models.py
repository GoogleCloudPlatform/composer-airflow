import copy
from datetime import datetime, timedelta
import getpass
import imp
import jinja2
import logging
import os
import dill
import re
import signal
import socket

from sqlalchemy import (
    Column, Integer, String, DateTime, Text, Boolean, ForeignKey, PickleType,
    Index,)
from sqlalchemy import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.orm import relationship

from airflow.executors import DEFAULT_EXECUTOR, LocalExecutor
from airflow.configuration import conf
from airflow import settings
from airflow import utils
from airflow.utils import State
from airflow.utils import apply_defaults

Base = declarative_base()
ID_LEN = 250
SQL_ALCHEMY_CONN = conf.get('core', 'SQL_ALCHEMY_CONN')
DAGS_FOLDER = os.path.expanduser(conf.get('core', 'DAGS_FOLDER'))

if 'mysql' in SQL_ALCHEMY_CONN:
    LongText = LONGTEXT
else:
    LongText = Text


def clear_task_instances(tis, session):
    '''
    Clears a set of task instances, but makes sure the running ones
    get killed.
    '''
    job_ids = []
    for ti in tis:
        if ti.state == State.RUNNING:
            if ti.job_id:
                ti.state = State.SHUTDOWN
                job_ids.append(ti.job_id)
        else:
            session.delete(ti)
    if job_ids:
        from airflow.jobs import BaseJob as BJ  # HA!
        for job in session.query(BJ).filter(BJ.id.in_(job_ids)).all():
            job.state = State.SHUTDOWN


class DagBag(object):
    """
    A dagbag is a collection of dags, parsed out of a folder tree and has high
    level configuration settings, like what database to use as a backend and
    what executor to use to fire off tasks. This makes it easier to run
    distinct environments for say production and development, tests, or for
    different teams or security profiles. What would have been system level
    settings are now dagbag level so that one system can run multiple,
    independent settings sets.
    """
    def __init__(
            self,
            dag_folder=None,
            executor=DEFAULT_EXECUTOR,
            include_examples=True):
        if not dag_folder:
            dag_folder = DAGS_FOLDER
        logging.info("Filling up the DagBag from " + dag_folder)
        self.dag_folder = dag_folder
        self.dags = {}
        self.file_last_changed = {}
        self.executor = executor
        self.collect_dags(dag_folder)
        if include_examples:
            example_dag_folder = os.path.join(
                os.path.dirname(__file__),
                'example_dags')
            self.collect_dags(example_dag_folder)
        self.merge_dags()

    def process_file(self, filepath, only_if_updated=True, safe_mode=True):
        """
        Given a path to a python module, this method imports the module and
        look for dag objects whithin it.
        """
        dttm = datetime.fromtimestamp(os.path.getmtime(filepath))
        mod_name, file_ext = os.path.splitext(os.path.split(filepath)[-1])

        if safe_mode:
            # Skip file if no obvious references to airflow or DAG are found.
            f = open(filepath, 'r')
            content = f.read()
            f.close()
            if not all([s in content for s in ('DAG', 'airflow')]):
                return

        if (
                not only_if_updated or
                filepath not in self.file_last_changed or
                dttm != self.file_last_changed[filepath]):
            try:
                logging.info("Importing " + filepath)
                m = imp.load_source(mod_name, filepath)
            except:
                logging.error("Failed to import: " + filepath)
                self.file_last_changed[filepath] = dttm
                return

            for dag in m.__dict__.values():
                if type(dag) == DAG:
                    dag.full_filepath = filepath
                    self.bag_dag(dag)

            self.file_last_changed[filepath] = dttm

    def bag_dag(self, dag):
        '''
        Adds the DAG into the bag, recurses into sub dags.
        '''
        self.dags[dag.dag_id] = dag
        dag.resolve_template_files()
        for task in dag.tasks:
            # Late import to prevent circular imports
            from airflow.operators import SubDagOperator
            if isinstance(task, SubDagOperator):
                task.subdag.full_filepath = dag.full_filepath
                task.subdag.parent_dag = dag
                self.bag_dag(task.subdag)
        logging.info('Loaded DAG {dag}'.format(**locals()))

    def collect_dags(
            self,
            dag_folder=DAGS_FOLDER,
            only_if_updated=True):
        """
        Given a file path or a folder, this file looks for python modules,
        imports them and adds them to the dagbag collection.

        Note that if a .airflowignore file is found while processing,
        the directory, it will behaves much like a .gitignore does,
        ignoring files that match any of the regex patterns specified
        in the file.
        """
        if os.path.isfile(dag_folder):
            self.process_file(dag_folder, only_if_updated=only_if_updated)
        elif os.path.isdir(dag_folder):
            patterns = []
            for root, dirs, files in os.walk(dag_folder):
                ignore_file = [f for f in files if f == '.airflowignore']
                if ignore_file:
                    f = open(os.path.join(root, ignore_file[0]), 'r')
                    patterns += [p for p in f.read().split('\n') if p]
                    f.close()
                for f in files:
                    filepath = os.path.join(root, f)
                    if not os.path.isfile(filepath):
                        continue
                    mod_name, file_ext = os.path.splitext(
                        os.path.split(filepath)[-1])
                    if file_ext != '.py':
                        continue
                    if not any([re.findall(p, filepath) for p in patterns]):
                        self.process_file(
                            filepath, only_if_updated=only_if_updated)

    def merge_dags(self):
        session = settings.Session()
        for dag in self.dags.values():
            session.merge(dag)
        session.commit()
        session.close()

    def paused_dags(self):
        session = settings.Session()
        dag_ids = [dp.dag_id for dp in session.query(DAG).filter(
            DAG.is_paused == True)]
        session.commit()
        session.close()
        return dag_ids


class User(Base):
    """
    Eventually should be used for security purposes
    """
    __tablename__ = "user"
    id = Column(Integer, primary_key=True)
    username = Column(String(ID_LEN), unique=True)
    email = Column(String(500))

    def __init__(self, username=None, email=None):
        self.username = username
        self.email = email

    def __repr__(self):
        return self.username

    def get_id(self):
        return unicode(self.id)

    def is_active(self):
        return True

    def is_authenticated(self):
        return True

    def is_anonymous(self):
        return False


class Connection(Base):
    """
    Placeholder to store information about different database instances
    connection information. The idea here is that scripts use references to
    database instances (conn_id) instead of hard coding hostname, logins and
    passwords when using operators or hooks.
    """
    __tablename__ = "connection"

    id = Column(Integer(), primary_key=True)
    conn_id = Column(String(ID_LEN), unique=True)
    conn_type = Column(String(500))
    host = Column(String(500))
    schema = Column(String(500))
    login = Column(String(500))
    password = Column(String(500))
    port = Column(Integer())
    extra = Column(String(5000))

    def __init__(
            self, conn_id=None, conn_type=None,
            host=None, login=None, password=None,
            schema=None, port=None):
        self.conn_id = conn_id
        self.conn_type = conn_type
        self.host = host
        self.login = login
        self.password = password
        self.schema = schema
        self.port = port

    def get_hook(self):
        from airflow import hooks
        if self.conn_type == 'mysql':
            return hooks.MySqlHook(mysql_conn_id=self.conn_id)
        elif self.conn_type == 'postgres':
            return hooks.PostgresHook(postgres_conn_id=self.conn_id)
        elif self.conn_type == 'hive_cli':
            return hooks.HiveCliHook(hive_cli_conn_id=self.conn_id)
        elif self.conn_type == 'presto':
            return hooks.PrestoHook(presto_conn_id=self.conn_id)
        elif self.conn_type == 'hiveserver2':
            return hooks.HiveServer2Hook(hiveserver2_conn_id=self.conn_id)

    def __repr__(self):
        return self.conn_id


class DagPickle(Base):
    """
    Dags can originate from different places (user repos, master repo, ...)
    and also get executed in different places (different executors). This
    object represents a version of a DAG and becomes a source of truth for
    a BackfillJob execution. A pickle is a native python serialized object,
    and in this case gets stored in the database for the duration of the job.

    The executors pick up the DagPickle id and read the dag definition from
    the database.
    """
    id = Column(Integer, primary_key=True)
    pickle = Column(PickleType(pickler=dill))

    __tablename__ = "dag_pickle"

    def __init__(self, dag):
        self.dag_id = dag.dag_id
        if hasattr(dag, 'template_env'):
            dag.template_env = None
        self.pickle = dag


class TaskInstance(Base):
    """
    Task instances store the state of a task instance. This table is the
    autorithy and single source of truth around what tasks have run and the
    state they are in.

    The SqlAchemy model doesn't have a SqlAlchemy foreign key to the task or
    dag model deliberately to have more control over transactions.

    Database transactions on this table should insure double triggers and
    any confusion around what task instances are or aren't ready to run
    even while multiple schedulers may be firing task instances.
    """

    __tablename__ = "task_instance"

    task_id = Column(String(ID_LEN), primary_key=True)
    dag_id = Column(String(ID_LEN), primary_key=True)
    execution_date = Column(DateTime, primary_key=True)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    duration = Column(Integer)
    state = Column(String(20))
    try_number = Column(Integer)
    hostname = Column(String(1000))
    unixname = Column(String(1000))
    job_id = Column(Integer)

    __table_args__ = (
        Index('ti_dag_state', dag_id, state),
        Index('ti_state_lkp', dag_id, task_id, execution_date, state),
    )

    def __init__(self, task, execution_date, job=None):
        self.dag_id = task.dag_id
        self.task_id = task.task_id
        self.execution_date = execution_date
        self.task = task
        self.try_number = 1
        self.unixname = getpass.getuser()
        if job:
            self.job_id = job.id

    def command(
            self,
            mark_success=False,
            ignore_dependencies=False,
            force=False,
            local=False,
            pickle_id=None,
            raw=False,
            job_id=None):
        """
        Returns a command that can be executed anywhere where airflow is
        installed. This command is part of the message sent to executors by
        the orchestrator.
        """
        iso = self.execution_date.isoformat()
        mark_success = "--mark_success" if mark_success else ""
        pickle = "--pickle {0}".format(pickle_id) if pickle_id else ""
        job_id = "--job_id {0}".format(job_id) if job_id else ""
        ignore_dependencies = "-i" if ignore_dependencies else ""
        force = "--force" if force else ""
        local = "--local" if local else ""
        raw = "--raw" if raw else ""
        subdir = ""
        if not pickle and self.task.dag and self.task.dag.full_filepath:
            subdir = "-sd {0}".format(self.task.dag.full_filepath)
        return (
            "airflow run "
            "{self.dag_id} {self.task_id} {iso} "
            "{mark_success} "
            "{pickle} "
            "{local} "
            "{ignore_dependencies} "
            "{force} "
            "{job_id} "
            "{raw} "
            "{subdir} "
        ).format(**locals())

    @property
    def log_filepath(self):
        iso = self.execution_date.isoformat()
        log = os.path.expanduser(conf.get('core', 'BASE_LOG_FOLDER'))
        return (
            "{log}/{self.dag_id}/{self.task_id}/{iso}.log".format(**locals()))

    @property
    def log_url(self):
        iso = self.execution_date.isoformat()
        BASE_URL = conf.get('webserver', 'BASE_URL')
        return BASE_URL + (
            "/admin/airflow/log"
            "?dag_id={self.dag_id}"
            "&task_id={self.task_id}"
            "&execution_date={iso}"
        ).format(**locals())

    def current_state(self, main_session=None):
        """
        Get the very latest state from the database, if a session is passed,
        we use and looking up the state becomes part of the session, otherwise
        a new session is used.
        """
        session = main_session or settings.Session()
        TI = TaskInstance
        ti = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.task_id == self.task_id,
            TI.execution_date == self.execution_date,
        ).all()
        if ti:
            state = ti[0].state
        else:
            state = None
        if not main_session:
            session.commit()
            session.close()
        return state

    def error(self, main_session=None):
        """
        Forces the task instance's state to FAILED in the database.
        """
        session = settings.Session()
        logging.error("Recording the task instance as FAILED")
        self.state = State.FAILED
        session.merge(self)
        session.commit()
        session.close()

    def refresh_from_db(self, main_session=None):
        """
        Refreshes the task instance from the database based on the primary key
        """
        session = main_session or settings.Session()
        TI = TaskInstance
        ti = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.task_id == self.task_id,
            TI.execution_date == self.execution_date,
        ).first()
        if ti:
            self.state = ti.state
            self.start_date = ti.start_date
            self.end_date = ti.end_date
            self.try_number = ti.try_number

        if not main_session:
            session.commit()
            session.close()

    @property
    def key(self):
        """
        Returns a tuple that identifies the task instance uniquely
        """
        return (self.dag_id, self.task_id, self.execution_date)

    def is_runnable(self):
        """
        Returns a boolean on whether the task instance has met all dependencies
        and is ready to run. It considers the task's state, the state
        of its dependencies, depends_on_past and makes sure the execution
        isn't in the future.
        """
        if self.execution_date > datetime.now() - self.task.schedule_interval:
            return False
        elif self.state == State.UP_FOR_RETRY and not self.ready_for_retry():
            return False
        elif self.task.end_date and self.execution_date > self.task.end_date:
            return False
        elif self.state in State.runnable() and self.are_dependencies_met():
            return True
        else:
            return False

    def are_dependents_done(self, main_session=None):
        """
        Checks whether the dependents of this task instance have all succeeded.
        This is meant to be used by wait_for_downstream.

        This is useful when you do not want to start processing the next
        schedule of a task until the dependents are done. For instance,
        if the task DROPs and recreates a table.
        """
        session = main_session or settings.Session()
        task = self.task

        if not task._downstream_list:
            return True

        downstream_task_ids = [t.task_id for t in task._downstream_list]
        ti = session.query(func.count(TaskInstance.task_id)).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id.in_(downstream_task_ids),
            TaskInstance.execution_date == self.execution_date,
            TaskInstance.state == State.SUCCESS,
        )
        count = ti[0][0]
        if not main_session:
            session.commit()
            session.close()
        return count == len(task._downstream_list)

    def are_dependencies_met(self, main_session=None):
        """
        Returns a boolean on whether the upstream tasks are in a SUCCESS state
        and considers depends_on_past and the previous' run state.
        """
        TI = TaskInstance

        # Using the session if passed as param
        session = main_session or settings.Session()
        task = self.task

        # Checking that the depends_on_past is fulfilled
        if (task.depends_on_past and
                not self.execution_date == task.start_date):
            previous_ti = session.query(TI).filter(
                TI.dag_id == self.dag_id,
                TI.task_id == task.task_id,
                TI.execution_date ==
                self.execution_date-task.schedule_interval,
                TI.state == State.SUCCESS,
            ).first()
            if not previous_ti:
                return False

            # Applying wait_for_downstream
            previous_ti.task = self.task
            if task.wait_for_downstream and not \
                    previous_ti.are_dependents_done(session):
                return False

        # Checking that all upstream dependencies have succeeded
        if task._upstream_list:
            upstream_task_ids = [t.task_id for t in task._upstream_list]
            ti = session.query(func.count(TI.task_id)).filter(
                TI.dag_id == self.dag_id,
                TI.task_id.in_(upstream_task_ids),
                TI.execution_date == self.execution_date,
                TI.state == State.SUCCESS,
            )
            count = ti[0][0]
            if count < len(task._upstream_list):
                return False

        if not main_session:
            session.commit()
            session.close()
        return True

    def __repr__(self):
        return (
            "<TaskInstance: {ti.dag_id}.{ti.task_id} "
            "{ti.execution_date} [{ti.state}]>"
        ).format(ti=self)

    def ready_for_retry(self):
        """
        Checks on whether the task instance is in the right state and timeframe
        to be retried.
        """
        return self.state == State.UP_FOR_RETRY and \
            self.end_date + self.task.retry_delay < datetime.now()

    def run(
            self,
            verbose=True,
            ignore_dependencies=False,  # Doesn't check for deps, just runs
            force=False,  # Disregards previous successes
            mark_success=False,  # Don't run the task, act as if it succeeded
            test_mode=False,  # Doesn't record success or failure in the DB
            job_id=None,):
        """
        Runs the task instance.
        """
        task = self.task
        session = settings.Session()
        self.refresh_from_db(session)
        session.commit()
        self.job_id = job_id
        iso = datetime.now().isoformat()
        self.hostname = socket.gethostname()

        msg = "\n"
        msg += ("-" * 80)
        if self.state == State.UP_FOR_RETRY:
            msg += "\nRetry run {self.try_number} out of {task.retries} "
            msg += "starting @{iso}\n"
        else:
            msg += "\nNew run starting @{iso}\n"
        msg += ("-" * 80)
        logging.info(msg.format(**locals()))

        if not force and self.state == State.SUCCESS:
            logging.info(
                "Task {self} previously succeeded"
                " on {self.end_date}".format(**locals())
            )
        elif not ignore_dependencies and \
                not self.are_dependencies_met(session):
            logging.warning("Dependencies not met yet")
        elif self.state == State.UP_FOR_RETRY and \
                not self.ready_for_retry():
            next_run = (self.end_date + task.retry_delay).isoformat()
            logging.info(
                "Not ready for retry yet. " +
                "Next run after {0}".format(next_run)
            )
        elif force or self.state in State.runnable():
            if self.state == State.UP_FOR_RETRY:
                self.try_number += 1
            else:
                self.try_number = 1
            if not test_mode:
                session.add(Log(State.RUNNING, self))
            self.state = State.RUNNING
            self.start_date = datetime.now()
            self.end_date = None
            if not test_mode:
                session.merge(self)
            session.commit()
            if verbose:
                if mark_success:
                    msg = "Marking success for "
                else:
                    msg = "Executing "
                msg += "{self.task} for {self.execution_date}"

            try:
                logging.info(msg.format(self=self))
                if not mark_success:

                    task_copy = copy.copy(task)
                    self.task = task_copy

                    def signal_handler(signum, frame):
                        '''Setting kill signal handler'''
                        logging.error("Killing subprocess")
                        task_copy.on_kill()
                        raise Exception("Task received SIGTERM signal")
                    signal.signal(signal.SIGTERM, signal_handler)

                    self.render_templates()
                    task_copy.execute(context=self.get_template_context())
            except (Exception, StandardError, KeyboardInterrupt) as e:
                self.record_failure(e, test_mode)
                raise e

            # Recording SUCCESS
            session = settings.Session()
            self.end_date = datetime.now()
            self.set_duration()
            self.state = State.SUCCESS
            if not test_mode:
                session.add(Log(State.SUCCESS, self))
                session.merge(self)

        session.commit()

    def record_failure(self, error, test_mode=False):
        logging.exception(error)
        task = self.task
        session = settings.Session()
        self.end_date = datetime.now()
        self.set_duration()
        if not test_mode:
            session.add(Log(State.FAILED, self))

        # Let's go deeper
        try:
            if self.try_number <= task.retries:
                self.state = State.UP_FOR_RETRY
                if task.email_on_retry and task.email:
                    self.email_alert(error, is_retry=True)
            else:
                self.state = State.FAILED
                if task.email_on_failure and task.email:
                    self.email_alert(error, is_retry=False)
        except Exception as e2:
            logging.error(
                'Failed to send email to: ' + str(task.email))
            logging.error(str(e2))

        if not test_mode:
            session.merge(self)
        session.commit()
        logging.error(str(error))

    def get_template_context(self):
        task = self.task
        from airflow import macros
        tables = None
        if 'tables' in task.params:
            tables = task.params['tables']
        ds = self.execution_date.isoformat()[:10]
        yesterday_ds = (self.execution_date - timedelta(1)).isoformat()[:10]
        tomorrow_ds = (self.execution_date + timedelta(1)).isoformat()[:10]
        ds_nodash = ds.replace('-', '')
        ti_key_str = "{task.dag_id}__{task.task_id}__{ds_nodash}"
        ti_key_str = ti_key_str.format(**locals())

        params = {}
        if hasattr(task, 'dag') and task.dag.params:
            params.update(task.dag.params)
        if task.params:
            params.update(task.params)

        return {
            'dag': task.dag,
            'ds': ds,
            'yesterday_ds': yesterday_ds,
            'tomorrow_ds': tomorrow_ds,
            'END_DATE': ds,
            'ds_nodash': ds_nodash,
            'end_date': ds,
            'execution_date': self.execution_date,
            'latest_date': ds,
            'macros': macros,
            'params': params,
            'tables': tables,
            'task': task,
            'task_instance': self,
            'ti': self,
            'task_instance_key_str': ti_key_str
        }

    def render_templates(self):
        task = self.task
        jinja_context = self.get_template_context()
        if hasattr(self, 'task') and hasattr(self.task, 'dag'):
            if self.task.dag.user_defined_macros:
                jinja_context.update(
                    self.task.dag.user_defined_macros)

        for attr in task.__class__.template_fields:
            result = getattr(task, attr)
            template = self.task.get_template(attr)
            result = template.render(**jinja_context)
            setattr(task, attr, result)

    def email_alert(self, exception, is_retry=False):
        task = self.task
        title = "Airflow alert: {self}".format(**locals())
        exception = str(exception).replace('\n', '<br>')
        try_ = task.retries + 1
        body = (
            "Try {self.try_number} out of {try_}<br>"
            "Exception:<br>{exception}<br>"
            "Log: <a href='{self.log_url}'>Link</a><br>"
            "Host: {self.hostname}<br>"
            "Log file: {self.log_filepath}<br>"
        ).format(**locals())
        utils.send_email(task.email, title, body)

    def set_duration(self):
        if self.end_date and self.start_date:
            self.duration = (self.end_date - self.start_date).seconds
        else:
            self.duration = None


class Log(Base):
    """
    Used to actively log events to the database
    """

    __tablename__ = "log"

    id = Column(Integer, primary_key=True)
    dttm = Column(DateTime)
    dag_id = Column(String(ID_LEN))
    task_id = Column(String(ID_LEN))
    event = Column(String(30))
    execution_date = Column(DateTime)
    owner = Column(String(500))

    def __init__(self, event, task_instance):
        self.dttm = datetime.now()
        self.dag_id = task_instance.dag_id
        self.task_id = task_instance.task_id
        self.execution_date = task_instance.execution_date
        self.event = event
        self.owner = task_instance.task.owner


class BaseOperator(Base):
    """
    Abstract base class for all operators. Since operators create objects that
    become node in the dag, BaseOperator contains many recursive methods for
    dag crawling behavior. To derive this class, you are expected to override
    the constructor as well as the 'execute' method.

    Operators derived from this task should perform or trigger certain tasks
    synchronously (wait for completion). Example of operators could be an
    operator the runs a Pig job (PigOperator), a sensor operator that
    waits for a partition to land in Hive (HiveSensorOperator), or one that
    moves data from Hive to MySQL (Hive2MySqlOperator). Instances of these
    operators (tasks) target specific operations, running specific scripts,
    functions or data transfers.

    This class is abstract and shouldn't be instantiated. Instantiating a
    class derived from this one results in the creation of a task object,
    which ultimately becomes a node in DAG objects. Task dependencies should
    be set by using the set_upstream and/or set_downstream methods.

    Note that this class is derived from SQLAlquemy's Base class, which
    allows us to push metadata regarding tasks to the database. Deriving this
    classes needs to implement the polymorphic specificities documented in
    SQLAlchemy. This should become clear while reading the code for other
    operators.

    :param task_id: a unique, meaningful id for the task
    :type task_id: string
    :param owner: the owner of the task, using the unix username is recommended
    :type owner: string
    :param retries: the number of retries that should be performed before
        failing the task
    :type retries: int
    :param retry_delay: delay between retries
    :type retry_delay: timedelta
    :param start_date: start date for the task, the scheduler will start from
        this point in time
    :type start_date: datetime
    :param end_date: if specified, the scheduler won't go beyond this date
    :type end_date: datetime
    :param schedule_interval: interval at which to schedule the task
    :type schedule_interval: timedelta
    :param depends_on_past: when set to true, task instances will run
        sequentially while relying on the previous task's schedule to
        succeed. The task instance for the start_date is allowed to run.
    :type depends_on_past: boolean
    :param dag: a reference to the dag the task is attached to (if any)
    :type dag: DAG
    """

    # For derived classes to define which fields will get jinjaified
    template_fields = []
    # Defines wich files extensions to look for in the templated fields
    template_ext = []
    # Defines the color in the UI
    ui_color = '#fff'
    ui_fgcolor = '#000'

    __tablename__ = "task"

    dag_id = Column(String(ID_LEN), primary_key=True)
    task_id = Column(String(ID_LEN), primary_key=True)
    owner = Column(String(500))
    task_type = Column(String(20))
    start_date = Column(DateTime())
    end_date = Column(DateTime())
    depends_on_past = Column(Integer())

    __mapper_args__ = {
        'polymorphic_on': task_type,
        'polymorphic_identity': 'BaseOperator'
    }

    @apply_defaults
    def __init__(
            self,
            task_id,
            owner,
            email=None,
            email_on_retry=True,
            email_on_failure=True,
            retries=0,
            retry_delay=timedelta(seconds=300),
            start_date=None,
            end_date=None,
            schedule_interval=timedelta(days=1),
            depends_on_past=False,
            wait_for_downstream=False,
            dag=None,
            params=None,
            default_args=None,
            adhoc=False,
            *args,
            **kwargs):

        utils.validate_key(task_id)
        self.dag_id = dag.dag_id if dag else 'adhoc_' + owner
        self.task_id = task_id
        self.owner = owner
        self.email = email
        self.email_on_retry = email_on_retry
        self.email_on_failure = email_on_failure
        self.start_date = start_date
        self.end_date = end_date
        self.depends_on_past = depends_on_past
        self.wait_for_downstream = wait_for_downstream
        self._schedule_interval = schedule_interval
        self.retries = retries
        if isinstance(retry_delay, timedelta):
            self.retry_delay = retry_delay
        else:
            logging.info("retry_delay isn't timedelta object, assuming secs")
            self.retry_delay = timedelta(seconds=retry_delay)
        self.params = params or {}  # Available in templates!
        self.adhoc = adhoc
        if dag:
            dag.add_task(self)
            self.dag = dag

        # Private attributes
        self._upstream_list = []
        self._downstream_list = []

    @property
    def schedule_interval(self):
        """
        The schedule interval of the DAG always wins over individual tasks so
        that tasks whitin a DAG always line up. The task still needs a
        schedule_interval as it may not be attached to a DAG.
        """
        if hasattr(self, 'dag') and self.dag:
            return self.dag.schedule_interval
        else:
            return self._schedule_interval

    def execute(self, context):
        '''
        This is the main method to derive when creating an operator.
        Context is the same dictionary used as when rendering jinja templates.

        Refer to get_template_context for more context.
        '''
        raise NotImplemented()

    def on_kill(self):
        '''
        Override this method to cleanup subprocesses when a task instance
        gets killed. Any use of the threading, subprocess or multiprocessing
        module whithin an operator needs to be cleaned up or it will leave
        ghost processes behind.
        '''
        pass

    def get_template(self, attr):
        content = getattr(self, attr)
        if hasattr(self, 'dag'):
            env = self.dag.get_template_env()
        else:
            env = jinja2.Environment(cache_size=0)

        exts = self.__class__.template_ext
        if any([content.endswith(ext) for ext in exts]):
            template = env.get_template(content)
        else:
            template = env.from_string(content)
        return template

    def prepare_template(self):
        '''
        Hook that is trigerred after the templated fields get replaced
        by their content. If you need your operator to alter the
        content of the file before the template is rendered,
        it should override this method to do so.
        '''
        pass

    def resolve_template_files(self):
        # Getting the content of files for template_field / template_ext
        for attr in self.template_fields:
            content = getattr(self, attr)
            if any([content.endswith(ext) for ext in self.template_ext]):
                env = self.dag.get_template_env()
                try:
                    setattr(self, attr, env.loader.get_source(env, content)[0])
                except Exception as e:
                    logging.exception(e)
        self.prepare_template()

    @property
    def upstream_list(self):
        """@property: list of tasks directly upstream"""
        return self._upstream_list

    @property
    def downstream_list(self):
        """@property: list of tasks directly downstream"""
        return self._downstream_list

    def clear(
            self, start_date=None, end_date=None,
            upstream=False, downstream=False):
        """
        Clears the state of task instances associated with the task, follwing
        the parameters specified.
        """
        session = settings.Session()

        TI = TaskInstance
        qry = session.query(TI).filter(TI.dag_id == self.dag_id)

        if start_date:
            qry = qry.filter(TI.execution_date >= start_date)
        if end_date:
            qry = qry.filter(TI.execution_date <= end_date)

        tasks = [self.task_id]

        if upstream:
            tasks += \
                [t.task_id for t in self.get_flat_relatives(upstream=True)]

        if downstream:
            tasks += \
                [t.task_id for t in self.get_flat_relatives(upstream=False)]

        qry = qry.filter(TI.task_id.in_(tasks))

        count = qry.count()
        clear_task_instances(qry, session)

        session.commit()
        session.close()
        return count

    def get_task_instances(self, session, start_date=None, end_date=None):
        """
        Get a set of task instance related to this task for a specific date
        range.
        """
        TI = TaskInstance
        end_date = end_date or datetime.now()
        return session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.task_id == self.task_id,
            TI.execution_date >= start_date,
            TI.execution_date <= end_date,
        ).order_by(TI.execution_date).all()

    def get_flat_relatives(self, upstream=False, l=None):
        """
        Get a flat list of relatives, either upstream or downstream.
        """
        if not l:
            l = []
        for t in self.get_direct_relatives(upstream):
            if t not in l:
                l.append(t)
                t.get_flat_relatives(upstream, l)
        return l

    def detect_downstream_cycle(self, task=None):
        """
        When invoked, this routine will raise an exception if a cycle is
        detected downstream from self. It is invoked when tasks are added to
        the DAG to detect cycles.
        """
        if not task:
            task = self
        for t in self.get_direct_relatives():
            if task == t:
                msg = "Cycle detect in DAG. Faulty task: {0}".format(task)
                raise Exception(msg)
            else:
                t.detect_downstream_cycle(task=task)
        return False

    def run(
            self, start_date=None, end_date=None, ignore_dependencies=False,
            force=False, mark_success=False):
        """
        Run a set of task instances for a date range.
        """
        start_date = start_date or self.start_date
        end_date = end_date or self.end_date or datetime.now()

        for dt in utils.date_range(
                start_date, end_date, self.schedule_interval):
            TaskInstance(self, dt).run(
                mark_success=mark_success,
                ignore_dependencies=ignore_dependencies,
                force=force,)

    def get_direct_relatives(self, upstream=False):
        """
        Get the direct relatives to the current task, upstream or
        downstream.
        """
        if upstream:
            return self.upstream_list
        else:
            return self.downstream_list

    def __repr__(self):
        return "<Task({self.task_type}): {self.task_id}>".format(self=self)

    def append_only_new(self, l, item):
        if item in l:
            raise Exception(
                'Dependency {self}, {item} already registered'
                ''.format(**locals()))
        else:
            l.append(item)

    def _set_relatives(self, task_or_task_list, upstream=False):
        if isinstance(task_or_task_list, BaseOperator):
            task_or_task_list = [task_or_task_list]
        for task in task_or_task_list:
            if not isinstance(task_or_task_list, list):
                raise Exception('Expecting a task')
            if upstream:
                self.append_only_new(task._downstream_list, self)
                self.append_only_new(self._upstream_list, task)
            else:
                self.append_only_new(task._upstream_list, self)
                self.append_only_new(self._downstream_list, task)
        self.detect_downstream_cycle()

    def set_downstream(self, task_or_task_list):
        """
        Set a task, or a task task to be directly downstream from the current
        task.
        """
        self._set_relatives(task_or_task_list, upstream=False)

    def set_upstream(self, task_or_task_list):
        """
        Set a task, or a task task to be directly upstream from the current
        task.
        """
        self._set_relatives(task_or_task_list, upstream=True)


class DAG(Base):
    """
    A dag (directed acyclic graph) is a collection of tasks with directional
    dependencies. A dag also has a schedule, a start end an end date
    (optional). For each schedule, (say daily or hourly), the DAG needs to run
    each individual tasks as their dependencies are met. Certain tasks have
    the property of depending on their own past, meaning that they can't run
    until their previous schedule (and upstream tasks) are completed.

    DAGs essentially act as namespaces for tasks. A task_id can only be
    added once to a DAG.

    :param dag_id: The id of the DAG
    :type dag_id: string
    :param schedule_interval: Defines how often that DAG runs
    :type schedule_interval: datetime.timedelta
    :param start_date: The timestamp from which the sceduler will
        attempt to backfill
    :type start_date: datetime.datetime
    :param end_date: A date beyond which your DAG won't run, leave to None
        for open ended scheduling
    :type end_date: datetime.datetime
    :param template_searchpath: This list of folders (non relative)
        defines where jinja will look for your templates. Order matters.
        Note that jinja/airflow includes the path of your DAG file by
        default
    :type template_searchpath: string or list of stings
    :param user_defined_macros: a dictionary of macros that will be merged
    :type user_defined_macros: dict
    :param default_args: A dictionary of default parameters to be used
        as constructor keyword parameters when initialising operators.
        Note that operators have the same hook, and precede those defined
        here, meaning that if your dict contains `'depends_on_past': True`
        here and `'depends_on_past': False` in te operator's call
        `default_args`, the actual value will be `False`.
    :type default_args: dict
    """

    __tablename__ = "dag"

    dag_id = Column(String(ID_LEN), primary_key=True)
    is_paused = Column(Boolean, default=False)

    def __init__(
            self, dag_id,
            schedule_interval=timedelta(days=1),
            start_date=None, end_date=None,
            full_filepath=None,
            template_searchpath=None,
            user_defined_macros=None,
            default_args=None,
            params=None):

        utils.validate_key(dag_id)
        self.tasks = []
        self.dag_id = dag_id
        self.start_date = start_date
        self.end_date = end_date or datetime.now()
        self.schedule_interval = schedule_interval
        self.full_filepath = full_filepath if full_filepath else ''
        if isinstance(template_searchpath, basestring):
            template_searchpath = [template_searchpath]
        self.template_searchpath = template_searchpath
        self.user_defined_macros = user_defined_macros
        self.default_args = default_args or {}
        self.params = params
        self.parent_dag = None  # Gets set when DAGs are loaded

    def __repr__(self):
        return "<DAG: {self.dag_id}>".format(self=self)

    @property
    def task_ids(self):
        return [t.task_id for t in self.tasks]

    @property
    def filepath(self):
        fn = self.full_filepath.replace(DAGS_FOLDER + '/', '')
        fn = fn.replace(os.path.dirname(__file__) + '/', '')
        return fn

    @property
    def folder(self):
        return os.path.dirname(self.full_filepath)

    @property
    def owner(self):
        return ", ".join(list(set([t.owner for t in self.tasks])))

    @property
    def latest_execution_date(self):
        TI = TaskInstance
        session = settings.Session()
        execution_date = session.query(func.max(TI.execution_date)).filter(
            TI.dag_id == self.dag_id,
            TI.task_id.in_(self.task_ids)
        ).scalar()
        session.commit()
        session.close()
        return execution_date

    def resolve_template_files(self):
        for t in self.tasks:
            t.resolve_template_files()

    def crawl_for_tasks(objects):
        """
        Typically called at the end of a script by passing globals() as a
        parameter. This allows to not explicitely add every single task to the
        dag explicitely.
        """
        raise NotImplemented("")

    def override_start_date(self, start_date):
        """
        Sets start_date of all tasks and of the DAG itself to a certain date.
        This is used by BackfillJob.
        """
        for t in self.tasks:
            t.start_date = start_date
        self.start_date = start_date

    def get_template_env(self):
        '''
        Returns a jinja2 Environment while taking into account the DAGs
        template_searchpath and user_defined_macros
        '''
        searchpath = [self.folder]
        if self.template_searchpath:
            searchpath += self.template_searchpath

        env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(searchpath),
            extensions=["jinja2.ext.do"],
            cache_size=0)
        if self.user_defined_macros:
            env.globals.update(self.user_defined_macros)

        return env

    def set_dependency(self, upstream_task_id, downstream_task_id):
        """
        Simple utility method to set dependency between two tasks that
        already have been added to the DAG using add_task()
        """
        self.get_task(upstream_task_id).set_downstream(
            self.get_task(downstream_task_id))

    def get_task_instances(self, session, start_date=None, end_date=None):
        TI = TaskInstance
        if not start_date:
            start_date = (datetime.today()-timedelta(30)).date()
            start_date = datetime.combine(start_date, datetime.min.time())
        if not end_date:
            end_date = datetime.now()
        tis = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.execution_date >= start_date,
            TI.execution_date <= end_date,
        ).all()
        return tis

    @property
    def roots(self):
        return [t for t in self.tasks if not t.downstream_list]

    def clear(
            self, start_date=None, end_date=None,
            upstream=False, downstream=False,
            only_failed=False,
            only_running=False,
            confirm_prompt=False):
        session = settings.Session()
        """
        Clears a set of task instances associated with the current dag for
        a specified date range.
        """

        TI = TaskInstance
        tis = session.query(TI).filter(TI.dag_id == self.dag_id)
        tis = tis.filter(TI.task_id.in_(self.task_ids))

        if start_date:
            tis = tis.filter(TI.execution_date >= start_date)
        if end_date:
            tis = tis.filter(TI.execution_date <= end_date)
        if only_failed:
            tis = tis.filter(TI.state == State.FAILED)
        if only_running:
            tis = tis.filter(TI.state == State.RUNNING)

        count = tis.count()
        if count == 0:
            print("Nothing to clear.")
            return 0
        if confirm_prompt:
            ti_list = "\n".join([str(t) for t in tis])
            question = (
                "You are about to delete these {count} tasks:\n"
                "{ti_list}\n\n"
                "Are you sure? (yes/no): ").format(**locals())
            if utils.ask_yesno(question):
                clear_task_instances(tis, session)
            else:
                count = 0
                print("Bail. Nothing was cleared.")
        else:
            clear_task_instances(tis, session)

        session.commit()
        session.close()
        return count

    def sub_dag(
            self, task_regex,
            include_downstream=False, include_upstream=True):
        """
        Returns a subset of the current dag as a deep copy of the current dag
        based on a regex that should match one or many tasks, and includes
        upstream and downstream neighboors based on the flag passed.
        """

        # Swiwtcharoo to go around deepcopying objects coming through the
        # backdoor
        user_defined_macros = self.user_defined_macros
        params = self.params
        delattr(self, 'user_defined_macros')
        delattr(self, 'params')
        dag = copy.deepcopy(self)
        self.user_defined_macros = user_defined_macros
        self.params = params

        regex_match = [
            t for t in dag.tasks if re.findall(task_regex, t.task_id)]
        also_include = []
        for t in regex_match:
            if include_downstream:
                also_include += t.get_flat_relatives(upstream=False)
            if include_upstream:
                also_include += t.get_flat_relatives(upstream=True)
        # Compiling the unique list of tasks that made the cut
        tasks = list(set(regex_match + also_include))
        dag.tasks = tasks
        for t in dag.tasks:
            # Removing upstream/downstream references to tasks that did not
            # made the cut
            t._upstream_list = [
                ut for ut in t._upstream_list if ut in tasks]
            t._downstream_list = [
                ut for ut in t._downstream_list if ut in tasks]

        return dag

    def get_task(self, task_id):
        for task in self.tasks:
            if task.task_id == task_id:
                return task
        raise Exception("Task {task_id} not found".format(**locals()))

    def tree_view(self):
        """
        Shows an ascii tree representation of the DAG
        """
        def get_downstream(task, level=0):
            print (" " * level * 4) + str(task)
            level += 1
            for t in task.upstream_list:
                get_downstream(t, level)

        for t in self.roots:
            get_downstream(t)

    def add_task(self, task):
        '''
        Add a task to the DAG

        :param task: the task you want to add
        :type task: task
        '''
        if not self.start_date and not task.start_date:
            raise Exception("Task is missing the start_date parameter")
        if not task.start_date:
            task.start_date = self.start_date

        if task.task_id in [t.task_id for t in self.tasks]:
            raise Exception(
                "Task id '{0}' has already been added "
                "to the DAG ".format(task.task_id))
        else:
            self.tasks.append(task)
            task.dag_id = self.dag_id
            task.dag = self
        self.task_count = len(self.tasks)

    def add_tasks(self, tasks):
        '''
        Add a list of tasks to the DAG

        :param task: a lit of tasks you want to add
        :type task: list of tasks
        '''
        for task in tasks:
            self.add_task(task)

    def db_merge(self):
        BO = BaseOperator
        session = settings.Session()
        tasks = session.query(BO).filter(BO.dag_id == self.dag_id).all()
        for t in tasks:
            session.delete(t)
        session.commit()
        session.merge(self)
        session.commit()

    def run(
            self, start_date=None, end_date=None, mark_success=False,
            include_adhoc=False, local=False, executor=None):
        from airflow.jobs import BackfillJob
        if not executor and local:
            executor = LocalExecutor()
        elif not executor:
            executor = DEFAULT_EXECUTOR
        job = BackfillJob(
            self,
            start_date=start_date,
            end_date=end_date,
            mark_success=mark_success,
            include_adhoc=include_adhoc,
            executor=executor)
        job.run()


class Chart(Base):
    __tablename__ = "chart"

    id = Column(Integer, primary_key=True)
    label = Column(String(200))
    conn_id = Column(
        String(ID_LEN), ForeignKey('connection.conn_id'), nullable=False)
    user_id = Column(Integer(), ForeignKey('user.id'),)
    chart_type = Column(String(100), default="line")
    sql_layout = Column(String(50), default="series")
    sql = Column(Text, default="SELECT series, x, y FROM table")
    y_log_scale = Column(Boolean)
    show_datatable = Column(Boolean)
    show_sql = Column(Boolean, default=True)
    height = Column(Integer, default=600)
    default_params = Column(String(5000), default="{}")
    owner = relationship(
        "User", cascade=False, cascade_backrefs=False, backref='charts')
    x_is_date = Column(Boolean, default=True)
    db = relationship("Connection")
    iteration_no = Column(Integer, default=0)
    last_modified = Column(DateTime, default=datetime.now())


class KnownEventType(Base):
    __tablename__ = "known_event_type"

    id = Column(Integer, primary_key=True)
    know_event_type = Column(String(200))

    def __repr__(self):
        return self.know_event_type

class KnownEvent(Base):
    __tablename__ = "known_event"

    id = Column(Integer, primary_key=True)
    label = Column(String(200))
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    user_id = Column(Integer(), ForeignKey('user.id'),)
    known_event_type_id = Column(Integer(), ForeignKey('known_event_type.id'),)
    reported_by = relationship(
        "User", cascade=False, cascade_backrefs=False, backref='known_events')
    event_type = relationship(
        "KnownEventType", cascade=False, cascade_backrefs=False, backref='known_events')
    description = Column(Text)
