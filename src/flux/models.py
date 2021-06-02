import copy
from datetime import datetime, timedelta
import imp
import inspect
import jinja2
import logging
import os
import pickle
import re
from time import sleep

from sqlalchemy import (
    Column, Integer, String, DateTime, Text,
    ForeignKey, func
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from flux import macros
from flux.executors import DEFAULT_EXECUTOR
from flux import settings
from flux import utils
from settings import ID_LEN
import socket
from utils import State

Base = declarative_base()


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
            dag_folder=settings.DAGS_FOLDER,
            executor=DEFAULT_EXECUTOR):
        self.dags = {}
        logging.info("Filling up the DagBag from " + dag_folder)
        self.collect_dags(dag_folder)
        self.executor = executor

    def process_file(self, filepath):
        mod_name, file_ext = os.path.splitext(os.path.split(filepath)[-1])
        if file_ext != '.py':
            return
        try:
            logging.info("Importing " + filepath)
            m = imp.load_source(mod_name, filepath)
        except:
            logging.error("Failed to import: " + filepath)
        else:
            for dag in m.__dict__.values():
                if type(dag) == DAG:
                    if dag.dag_id in self.dags:
                        raise Exception(
                            'Two DAGs with the same dag_id. No good.')
                    self.dags[dag.dag_id] = dag
                    dag.dagbag = self
                    if settings.RUN_AS_MASTER:
                        dag.db_merge()

    def collect_dags(self, file_location):
        if os.path.isfile(file_location):
            self.process_file(file_location)
        elif os.path.isdir(file_location):
            for root, dirs, files in os.walk(file_location):
                for f in files:
                    filepath = root + '/' + f
                    self.process_file(filepath)


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


class DatabaseConnection(Base):
    """
    Placeholder to store information about different database instances
    connection information. The idea here is that scripts use references to
    database instances (db_id) when using operators or hooks.
    """
    __tablename__ = "db_connection"

    id = Column(Integer, primary_key=True)
    db_id = Column(String(ID_LEN), unique=True)
    db_type = Column(String(500))
    host = Column(String(500))
    schema = Column(String(500))
    login = Column(String(500))
    password = Column(String(500))

    def __init__(
            self, db_id=None, db_type=None,
            host=None, login=None, password=None,
            schema=None):
        self.db_id = db_id
        self.db_type = db_type
        self.host = host
        self.login = login
        self.password = password
        self.schema = schema


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
    pickle = Column(Text())

    __tablename__ = "dag_pickle"
    
    def __init__(self, dag, job):
        self.pickle = dag.pickle()
        self.dag_id = dag.dag_id
        self.job = job

    def get_object(self):
        return pickle.loads(self.pickle)


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

    def __init__(self, task, execution_date):
        self.dag_id = task.dag_id
        self.task_id = task.task_id
        self.execution_date = execution_date
        self.task = task
        self.try_number = 1

    def command(self, mark_success=False, pickle=None):
        iso = self.execution_date.isoformat()
        mark_success = "--mark_success" if mark_success else ""
        pickle = "--pickle {0}".format(pickle.id)  if pickle else ""
        subdir = ""
        if not pickle and self.dag and self.dag.filepath:
            subdir = "-sd {self.task.dag.filepath}" 
        return (
            "./flux run "
            "{self.dag_id} {self.task_id} {iso} "
            "{mark_success} "
            "{pickle} "
            "{subdir} "
        ).format(**locals())

    @property
    def log_filepath(self):
        iso = self.execution_date.isoformat()
        return settings.BASE_LOG_FOLDER + \
            "/{self.dag_id}/{self.task_id}/{iso}.log".format(**locals())

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
        Fails the task
        """
        session = settings.Session()
        logging.error("Recording the task instance as FAILED")
        self.state = State.FAILED
        session.merge(self)
        session.commit()
        session.close()

    def refresh_from_db(self, main_session=None):
        session = main_session or settings.Session()
        TI = TaskInstance
        ti = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.task_id == self.task_id,
            TI.execution_date == self.execution_date,
        ).all()
        if ti:
            ti = ti[0]
            self.state = ti.state
            self.start_date = ti.start_date
            self.end_date = ti.end_date
            self.try_number = ti.try_number

        if not main_session:
            session.commit()
            session.close()

    @property
    def key(self):
        return (self.dag_id, self.task_id, self.execution_date)

    def is_runnable(self):
        if self.execution_date > datetime.now() - self.task.schedule_interval:
            return False
        elif self.state == State.UP_FOR_RETRY and not self.ready_for_retry():
            return False
        elif self.state in State.runnable() and self.are_dependencies_met():
            return True
        else:
            return False

    def are_dependencies_met(self, main_session=None):
        """
        Returns a boolean on whether this task is ready to run.
        """

        # Using the session if passed as param
        session = main_session or settings.Session()
        task = self.task

        # Checking that the depends_on_past is fulfilled
        if (task.depends_on_past and
                not task.execution_date == task.start_date):
            current_state = self.current_state()
            if current_state == State.SUCCESS:
                return False

        # Checking that all upstream dependencies have succeeded
        if task._upstream_list:
            upstream_task_ids = [t.task_id for t in task._upstream_list]
            ti = session.query(TaskInstance).filter(
                TaskInstance.dag_id == self.dag_id,
                TaskInstance.task_id.in_(upstream_task_ids),
                TaskInstance.execution_date == self.execution_date,
                TaskInstance.state == State.SUCCESS,
            )
            if ti.count() < len(task._upstream_list):
                return False

        if not main_session:
            session.commit()
            session.close()
        return True

    def __repr__(self):
        return (
            "<TaskInstance: "
            "{ti.dag_id}.{ti.task_id} {ti.execution_date}>"
        ).format(ti=self)

    def ready_for_retry(self):
        return self.state == State.UP_FOR_RETRY and \
            self.end_date + self.task.retry_delay < datetime.now()

    def run(
            self, verbose=True,
            ignore_dependencies=False,
            force=False,
            mark_success=False,
            ):
        session = settings.Session()
        self.refresh_from_db(session)
        iso = datetime.now().isoformat()

        msg = "\n"
        msg += ("-" * 80)
        if self.state == State.UP_FOR_RETRY:
            msg += "\nRetry run {self.try_number} out of {self.task.retries} "
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
            next_run = (self.end_date + self.task.retry_delay).isoformat()
            logging.info(
                "Not ready for retry yet. " +
                "Next run after {0}".format(next_run)
            )
        elif self.state in State.runnable():
            if self.state == State.UP_FOR_RETRY:
                self.try_number += 1
            else:
                self.try_number = 1
            session.add(Log(State.RUNNING, self))
            self.state = State.RUNNING
            self.start_date = datetime.now()
            self.end_date = None
            session.merge(self)
            session.commit()
            if verbose:
                if mark_success:
                    msg = "Marking success for "
                else:
                    msg = "Executing "
                msg += "{self.task} for {self.execution_date}"
                logging.info(msg.format(self=self))
            try:
                if not mark_success:
                    jinja_context = {
                        'ti': self,
                        'execution_date': self.execution_date,
                        'task': self.task,
                        'dag': self.task.dag,
                        'macros': macros,
                        'params': self.task.params,
                    }
                    task_copy = copy.copy(self.task)
                    for attr in task_copy.__class__.template_fields:
                        source = getattr(task_copy, attr)
                        setattr(
                            task_copy, attr,
                            jinja2.Template(source).render(**jinja_context)
                        )
                    task_copy.execute(self.execution_date)
            except Exception as e:
                self.end_date = datetime.now()
                self.set_duration()
                session.add(Log(State.FAILED, self))
                if self.try_number <= self.task.retries:
                    self.state = State.UP_FOR_RETRY
                else:
                    self.state = State.FAILED
                session.merge(self)
                session.commit()
                raise e

            self.end_date = datetime.now()
            self.set_duration()
            self.state = State.SUCCESS
            session.add(Log(State.SUCCESS, self))
            session.merge(self)

        session.commit()

    def set_duration(self):
        if self.end_date and self.start_date:
            self.duration = (self.end_date - self.start_date).seconds
        else:
            self.duration = None


class Log(Base):

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


class BaseJob(Base):

    __tablename__ = "job"

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(ID_LEN), ForeignKey('dag.dag_id'))
    state = Column(String(20))
    job_type = Column(String(30))
    start_date = Column(DateTime())
    end_date = Column(DateTime())
    latest_heartbeat = Column(DateTime())
    executor_class = Column(String(500))
    hostname = Column(String(500))

    __mapper_args__ = {
        'polymorphic_on': job_type,
        'polymorphic_identity': 'BaseJob'
    }

    def __init__(self, executor=DEFAULT_EXECUTOR):
        self.state = None
        self.hostname = socket.gethostname()
        self.executor_class = executor.__name__
        self.executor = executor()
        self.executor.start()
        self.start_date = datetime.now()
        self.latest_heartbeat = datetime.now()

    def is_alive(self):
        return (
            (datetime.now() - self.latest_heartbeat).seconds <
            (settings.JOB_HEARTBEAT_SEC * 2.1)
        )

    def heartbeat(self):
        session = settings.Session()
        sleep_for = settings.JOB_HEARTBEAT_SEC - (
            datetime.now() - self.latest_heartbeat).total_seconds()
        if sleep_for > 0:
            sleep(sleep_for)
        self.latest_heartbeat = datetime.now()
        session.merge(self)
        session.commit()
        session.close()

    def run(self):
        raise NotImplemented("This method needs to be overriden")

class BackfillJob(BaseJob):

    __mapper_args__ = {
        'polymorphic_identity': 'BackfillJob'
    }

    def run(self, dag, start_date=None, end_date=None, mark_success=False):
        session = settings.Session()

        # Build a list of all intances to run
        task_instances = {}
        self.dag = dag

        pickle = DagPickle(dag, self)
        session.add(pickle)
        session.commit()

        executor = self.executor
        for task in self.dag.tasks:
            start_date = start_date or task.start_date
            end_date = end_date or task.end_date or datetime.now()
            for dttm in utils.date_range(
                    start_date, end_date, task.dag.schedule_interval):
                ti = TaskInstance(task, dttm)
                ti.refresh_from_db()
                if ti.state != State.SUCCESS:
                    task_instances[ti.key] = ti

        # Triggering what needs to get triggered
        while task_instances:
            for key, ti in task_instances.items():
                if ti.state == State.SUCCESS:
                    del task_instances[key]
                elif ti.is_runnable():
                    executor.queue_command(
                        key=ti.key, command=ti.command(
                            mark_success=mark_success,
                            pickle=pickle)
                    )
                    ti.state = State.RUNNING
            if task_instances:
                self.heartbeat()
            executor.heartbeat()

            for key, state in executor.get_event_buffer().items():
                dag_id, task_id, execution_date = key
                ti = task_instances[key]
                ti.refresh_from_db()
                if ti.state == State.FAILED:
                    # Removing downstream tasks from the one that has failed
                    logging.error("Task instance " + str(key) + " failed")
                    downstream = [
                        t.task_id
                        for t in dag.get_task(task_id).get_flat_relatives(
                            upstream=False)]
                    del task_instances[key]
                    for task_id in downstream:
                        key = (ti.dag_id, task_id, execution_date)
                        del task_instances[key]
                elif ti.state == State.SUCCESS:
                    del task_instances[key]
        executor.end()
        logging.info("Run summary:")
        session.close()


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
    """

    template_fields = []

    __tablename__ = "task"

    dag_id = Column(String(ID_LEN), ForeignKey('dag.dag_id'), primary_key=True)
    task_id = Column(String(ID_LEN), primary_key=True)
    owner = Column(String(500))
    task_type = Column(String(20))
    start_date = Column(DateTime())
    end_date = Column(DateTime())
    depends_on_past = Column(Integer)

    __mapper_args__ = {
        'polymorphic_on': task_type,
        'polymorphic_identity': 'BaseOperator'
    }

    def __init__(
            self,
            task_id,
            owner,
            dag_id=None,
            retries=0,
            retry_delay=timedelta(seconds=10),
            start_date=None,
            end_date=None,
            schedule_interval=timedelta(days=1),
            depends_on_past=False,
            dag=None,
            params=None,
            *args,
            **kwargs):

        utils.validate_key(task_id)
        self.dag_id = dag_id or 'adhoc_' + owner
        self.task_id = task_id
        self.owner = owner
        self.start_date = start_date
        self.end_date = end_date
        self.depends_on_past = depends_on_past
        self._schedule_interval = schedule_interval
        self.retries = retries
        self.retry_delay = retry_delay
        self.params = params or {}  # Available in templates!

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

    @property
    def upstream_list(self):
        return self._upstream_list

    @property
    def downstream_list(self):
        return self._downstream_list

    def pickle(self):
        return pickle.dumps(self)

    def clear(
            self, start_date=None, end_date=None,
            upstream=False, downstream=False):
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
        qry.delete(synchronize_session='fetch')

        session.commit()
        session.close()
        return count

    def get_task_instances(self, start_date=None, end_date=None):
        session = settings.Session()
        TI = TaskInstance
        end_date = end_date or datetime.now()
        return session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.task_id == self.task_id,
            TI.execution_date >= start_date,
            TI.execution_date <= end_date,
        ).order_by(TI.execution_date).all()

    def get_flat_relatives(self, upstream=False):
        l = []
        for t in self.get_direct_relatives(upstream):
            if t not in l:
                l.append(t)
                l += t.get_direct_relatives(upstream)
        return l

    def detect_downstream_cycle(self, task=None):
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
        start_date = start_date or self.start_date
        end_date = end_date or self.end_date or datetime.now()

        for dt in utils.date_range(
                start_date, end_date, self.schedule_interval):
            TaskInstance(self, dt).run(
                mark_success=mark_success,
                ignore_dependencies=ignore_dependencies,
                force=force,)

    def get_direct_relatives(self, upstream=False):
        if upstream:
            return self.upstream_list
        else:
            return self.downstream_list

    def __repr__(self):
        return "<Task({self.task_type}): {self.task_id}>".format(self=self)

    @staticmethod
    def append_only_new(l, item):
        if item in l:
            raise Exception('Dependency already registered')
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
        self._set_relatives(task_or_task_list, upstream=False)

    def set_upstream(self, task_or_task_list):
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
    """

    __tablename__ = "dag"

    dag_id = Column(String(ID_LEN), primary_key=True)
    task_count = Column(Integer)
    parallelism = Column(Integer)
    filepath = Column(String(2000))

    tasks = relationship(
        "BaseOperator", cascade="merge, delete, delete-orphan", backref='dag')

    def __init__(
            self, dag_id,
            schedule_interval=timedelta(days=1),
            start_date=None, end_date=None, parallelism=0,
            executor=DEFAULT_EXECUTOR):

        utils.validate_key(dag_id)
        self.dag_id = dag_id
        self._executor = executor
        self.end_date = end_date or datetime.now()
        self.parallelism = parallelism
        self.schedule_interval = schedule_interval
        self.filepath = inspect.getouterframes(inspect.currentframe())[3][1]

    def __repr__(self):
        return "<DAG: {self.dag_id}>".format(self=self)

    @property
    def executor(self):
        if hasattr(self, 'dagbag'):
            return self.dagbag.executor
        else:
            return self._executor

    @property
    def task_ids(self):
        return [t.task_id for t in self.tasks]

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

    def crawl_for_tasks(objects):
        """
        Typically called at the end of a script by passing globals() as a
        parameter. This allows to not explicitely add every single task to the
        dag explicitely.
        """
        raise NotImplemented("")

    def pickle(self):
        return pickle.dumps(self)

    def get_task_instances(self, start_date=None, end_date=None):
        session = settings.Session()
        TI = TaskInstance
        if not start_date:
            start_date = (datetime.today()-timedelta(30)).date()
            start_date = datetime.combine(start_date, datetime.min.time())
        if not end_date:
            end_date = datetime.now()
        return session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.execution_date >= start_date,
            TI.execution_date <= end_date,
        ).all()

    @property
    def roots(self):
        return [t for t in self.tasks if not t.downstream_list]

    def clear(
            self, start_date=None, end_date=None,
            upstream=False, downstream=False):
        session = settings.Session()

        TI = TaskInstance
        tis = session.query(TI).filter(TI.dag_id == self.dag_id)

        if start_date:
            tis = tis.filter(TI.execution_date >= start_date)
        if end_date:
            tis = tis.filter(TI.execution_date <= end_date)

        count = tis.count()
        tis.delete()

        session.commit()
        session.close()
        return count

    def sub_dag(
            self, task_regex,
            include_downstream=False, include_upstream=True):

        dag = copy.deepcopy(self)
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

    def tree_view(self):

        def get_downstream(task, level=0):
            print (" " * level * 4) + str(task)
            level += 1
            for t in task.upstream_list:
                get_downstream(t, level)

        for t in self.roots:
            get_downstream(t)

    def add_task(self, task):
        if task.task_id in [t.task_id for t in self.tasks]:
            raise Exception("Task already added")
        else:
            self.tasks.append(task)
            task.dag_id = self.dag_id
        self.task_count = len(self.tasks)

    def db_merge(self):
        session = settings.Session()
        session.merge(self)
        session.commit()

    def run(self, start_date=None, end_date=None, mark_success=False):
        session = settings.Session()
        job = BackfillJob(executor=self.executor)
        session.add(job)
        session.commit()
        job.run(self, start_date, end_date, mark_success)
        job.state = State.SUCCESS
        job.end_date = datetime.now()
        session.merge(job)
        session.commit()

Base.metadata.create_all(settings.engine)
