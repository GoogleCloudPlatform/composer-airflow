from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import sys
from builtins import str, input, object
from past.builtins import basestring
from copy import copy
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta  # for doctest
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import formatdate
import errno
from functools import wraps
import imp
import importlib
import inspect
import json
import logging
import os
import re
import shutil
import signal
import six
import smtplib
from tempfile import mkdtemp

from alembic.config import Config
from alembic import command
from alembic.migration import MigrationContext

from contextlib import contextmanager

from sqlalchemy import event, exc
from sqlalchemy.pool import Pool

import numpy as np
from croniter import croniter

from airflow import settings
from airflow import configuration


class AirflowException(Exception):
    pass


class AirflowSensorTimeout(Exception):
    pass


class TriggerRule(object):
    ALL_SUCCESS = 'all_success'
    ALL_FAILED = 'all_failed'
    ALL_DONE = 'all_done'
    ONE_SUCCESS = 'one_success'
    ONE_FAILED = 'one_failed'
    DUMMY = 'dummy'

    @classmethod
    def is_valid(cls, trigger_rule):
        return trigger_rule in cls.all_triggers()

    @classmethod
    def all_triggers(cls):
        return [getattr(cls, attr)
                for attr in dir(cls)
                if not attr.startswith("__") and not callable(getattr(cls, attr))]


class State(object):
    """
    Static class with task instance states constants and color method to
    avoid hardcoding.
    """
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    SHUTDOWN = "shutdown"  # External request to shut down
    FAILED = "failed"
    UP_FOR_RETRY = "up_for_retry"
    UPSTREAM_FAILED = "upstream_failed"
    SKIPPED = "skipped"

    state_color = {
        QUEUED: 'gray',
        RUNNING: 'lime',
        SUCCESS: 'green',
        SHUTDOWN: 'blue',
        FAILED: 'red',
        UP_FOR_RETRY: 'gold',
        UPSTREAM_FAILED: 'orange',
        SKIPPED: 'pink',
    }

    @classmethod
    def color(cls, state):
        if state in cls.state_color:
            return cls.state_color[state]
        else:
            return 'white'

    @classmethod
    def color_fg(cls, state):
        color = cls.color(state)
        if color in ['green', 'red']:
            return 'white'
        else:
            return 'black'

    @classmethod
    def runnable(cls):
        return [
            None, cls.FAILED, cls.UP_FOR_RETRY, cls.UPSTREAM_FAILED,
            cls.SKIPPED, cls.QUEUED]


cron_presets = {
    '@hourly': '0 * * * *',
    '@daily': '0 0 * * *',
    '@weekly': '0 0 * * 0',
    '@monthly': '0 0 1 * *',
    '@yearly': '0 0 1 1 *',
}

def provide_session(func):
    """
    Function decorator that provides a session if it isn't provided.
    If you want to reuse a session or run the function as part of a
    database transaction, you pass it to the function, if not this wrapper
    will create one and close it for you.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        needs_session = False
        if 'session' not in kwargs:
            needs_session = True
            session = settings.Session()
            kwargs['session'] = session
        result = func(*args, **kwargs)
        if needs_session:
            session.expunge_all()
            session.commit()
            session.close()
        return result
    return wrapper


def pessimistic_connection_handling():
    @event.listens_for(Pool, "checkout")
    def ping_connection(dbapi_connection, connection_record, connection_proxy):
        '''
        Disconnect Handling - Pessimistic, taken from:
        http://docs.sqlalchemy.org/en/rel_0_9/core/pooling.html
        '''
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SELECT 1")
        except:
            raise exc.DisconnectionError()
        cursor.close()

@provide_session
def merge_conn(conn, session=None):
    from airflow import models
    C = models.Connection
    if not session.query(C).filter(C.conn_id == conn.conn_id).first():
        session.add(conn)
        session.commit()


def initdb():
    session = settings.Session()

    from airflow import models
    upgradedb()

    merge_conn(
        models.Connection(
            conn_id='airflow_db', conn_type='mysql',
            host='localhost', login='root',
            schema='airflow'))
    merge_conn(
        models.Connection(
            conn_id='airflow_ci', conn_type='mysql',
            host='localhost', login='root',
            schema='airflow_ci'))
    merge_conn(
        models.Connection(
            conn_id='beeline_default', conn_type='beeline', port="10000",
            host='localhost', extra="{\"use_beeline\": true, \"auth\": \"\"}",
            schema='default'))
    merge_conn(
        models.Connection(
            conn_id='bigquery_default', conn_type='bigquery'))
    merge_conn(
        models.Connection(
            conn_id='local_mysql', conn_type='mysql',
            host='localhost', login='airflow', password='airflow',
            schema='airflow'))
    merge_conn(
        models.Connection(
            conn_id='presto_default', conn_type='presto',
            host='localhost',
            schema='hive', port=3400))
    merge_conn(
        models.Connection(
            conn_id='hive_cli_default', conn_type='hive_cli',
            schema='default',))
    merge_conn(
        models.Connection(
            conn_id='hiveserver2_default', conn_type='hiveserver2',
            host='localhost',
            schema='default', port=10000))
    merge_conn(
        models.Connection(
            conn_id='metastore_default', conn_type='hive_metastore',
            host='localhost', extra="{\"authMechanism\": \"PLAIN\"}",
            port=9083))
    merge_conn(
        models.Connection(
            conn_id='mysql_default', conn_type='mysql',
            login='root',
            host='localhost'))
    merge_conn(
        models.Connection(
            conn_id='postgres_default', conn_type='postgres',
            login='postgres',
            schema='airflow',
            host='localhost'))
    merge_conn(
        models.Connection(
            conn_id='sqlite_default', conn_type='sqlite',
            host='/tmp/sqlite_default.db'))
    merge_conn(
        models.Connection(
            conn_id='http_default', conn_type='http',
            host='https://www.google.com/'))
    merge_conn(
        models.Connection(
            conn_id='mssql_default', conn_type='mssql',
            host='localhost', port=1433))
    merge_conn(
        models.Connection(
            conn_id='vertica_default', conn_type='vertica',
            host='localhost', port=5433))
    merge_conn(
        models.Connection(
            conn_id='webhdfs_default', conn_type='hdfs',
            host='localhost', port=50070))
    merge_conn(
        models.Connection(
            conn_id='ssh_default', conn_type='ssh',
            host='localhost'))

    # Known event types
    KET = models.KnownEventType
    if not session.query(KET).filter(KET.know_event_type == 'Holiday').first():
        session.add(KET(know_event_type='Holiday'))
    if not session.query(KET).filter(KET.know_event_type == 'Outage').first():
        session.add(KET(know_event_type='Outage'))
    if not session.query(KET).filter(
            KET.know_event_type == 'Natural Disaster').first():
        session.add(KET(know_event_type='Natural Disaster'))
    if not session.query(KET).filter(
            KET.know_event_type == 'Marketing Campaign').first():
        session.add(KET(know_event_type='Marketing Campaign'))
    session.commit()

    models.DagBag(sync_to_db=True)

    Chart = models.Chart
    chart_label = "Airflow task instance by type"
    chart = session.query(Chart).filter(Chart.label == chart_label).first()
    if not chart:
        chart = Chart(
            label=chart_label,
            conn_id='airflow_db',
            chart_type='bar',
            x_is_date=False,
            sql=(
                "SELECT state, COUNT(1) as number "
                "FROM task_instance "
                "WHERE dag_id LIKE 'example%' "
                "GROUP BY state"),
        )
        session.add(chart)


def upgradedb():
    logging.info("Creating tables")
    package_dir = os.path.abspath(os.path.dirname(__file__))
    directory = os.path.join(package_dir, 'migrations')
    config = Config(os.path.join(package_dir, 'alembic.ini'))
    config.set_main_option('script_location', directory)
    config.set_main_option('sqlalchemy.url',
                           configuration.get('core', 'SQL_ALCHEMY_CONN'))
    command.upgrade(config, 'heads')


def resetdb():
    '''
    Clear out the database
    '''
    from airflow import models

    logging.info("Dropping tables that exist")
    models.Base.metadata.drop_all(settings.engine)
    mc = MigrationContext.configure(settings.engine)
    if mc._version.exists(settings.engine):
        mc._version.drop(settings.engine)
    initdb()


def validate_key(k, max_length=250):
    if not isinstance(k, basestring):
        raise TypeError("The key has to be a string")
    elif len(k) > max_length:
        raise AirflowException(
            "The key has to be less than {0} characters".format(max_length))
    elif not re.match(r'^[A-Za-z0-9_\-\.]+$', k):
        raise AirflowException(
            "The key ({k}) has to be made of alphanumeric characters, dashes, "
            "dots and underscores exclusively".format(**locals()))
    else:
        return True


def date_range(
        start_date,
        end_date=None,
        num=None,
        delta=None):
    """
    Get a set of dates as a list based on a start, end and delta, delta
    can be something that can be added to ``datetime.datetime``
    or a cron expression as a ``str``

    :param start_date: anchor date to start the series from
    :type start_date: datetime.datetime
    :param end_date: right boundary for the date range
    :type end_date: datetime.datetime
    :param num: alternatively to end_date, you can specify the number of
        number of entries you want in the range. This number can be negative,
        output will always be sorted regardless
    :type num: int

    >>> date_range(datetime(2016, 1, 1), datetime(2016, 1, 3), delta=timedelta(1))
    [datetime.datetime(2016, 1, 1, 0, 0), datetime.datetime(2016, 1, 2, 0, 0), datetime.datetime(2016, 1, 3, 0, 0)]
    >>> date_range(datetime(2016, 1, 1), datetime(2016, 1, 3), delta='0 0 * * *')
    [datetime.datetime(2016, 1, 1, 0, 0), datetime.datetime(2016, 1, 2, 0, 0), datetime.datetime(2016, 1, 3, 0, 0)]
    >>> date_range(datetime(2016, 1, 1), datetime(2016, 3, 3), delta="0 0 0 * *")
    [datetime.datetime(2016, 1, 1, 0, 0), datetime.datetime(2016, 2, 1, 0, 0), datetime.datetime(2016, 3, 1, 0, 0)]
    """
    if not delta:
        return []
    if end_date and start_date > end_date:
        raise Exception("Wait. start_date needs to be before end_date")
    if end_date and num:
        raise Exception("Wait. Either specify end_date OR num")
    if not end_date and not num:
        end_date = datetime.now()

    delta_iscron = False
    if isinstance(delta, six.string_types):
        delta_iscron = True
        cron = croniter(delta, start_date)
    elif isinstance(delta, timedelta):
        delta = abs(delta)
    l = []
    if end_date:
        while start_date <= end_date:
            l.append(start_date)
            if delta_iscron:
                start_date = cron.get_next(datetime)
            else:
                start_date += delta
    else:
        for i in range(abs(num)):
            l.append(start_date)
            if delta_iscron:
                if num > 0:
                    start_date = cron.get_next(datetime)
                else:
                    start_date = cron.get_prev(datetime)
            else:
                if num > 0:
                    start_date += delta
                else:
                    start_date -= delta
    return sorted(l)


def json_ser(obj):
    """
    json serializer that deals with dates
    usage: json.dumps(object, default=utils.json_ser)
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()


def alchemy_to_dict(obj):
    """
    Transforms a SQLAlchemy model instance into a dictionary
    """
    if not obj:
        return None
    d = {}
    for c in obj.__table__.columns:
        value = getattr(obj, c.name)
        if type(value) == datetime:
            value = value.isoformat()
        d[c.name] = value
    return d


def readfile(filepath):
    f = open(filepath)
    content = f.read()
    f.close()
    return content


def apply_defaults(func):
    """
    Function decorator that Looks for an argument named "default_args", and
    fills the unspecified arguments from it.

    Since python2.* isn't clear about which arguments are missing when
    calling a function, and that this can be quite confusing with multi-level
    inheritance and argument defaults, this decorator also alerts with
    specific information about the missing arguments.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        if len(args) > 1:
            raise AirflowException(
                "Use keyword arguments when initializing operators")
        dag_args = {}
        dag_params = {}
        if 'dag' in kwargs and kwargs['dag']:
            dag = kwargs['dag']
            dag_args = copy(dag.default_args) or {}
            dag_params = copy(dag.params) or {}

        params = {}
        if 'params' in kwargs:
            params = kwargs['params']
        dag_params.update(params)

        default_args = {}
        if 'default_args' in kwargs:
            default_args = kwargs['default_args']
            if 'params' in default_args:
                dag_params.update(default_args['params'])
                del default_args['params']

        dag_args.update(default_args)
        default_args = dag_args
        arg_spec = inspect.getargspec(func)
        num_defaults = len(arg_spec.defaults) if arg_spec.defaults else 0
        non_optional_args = arg_spec.args[:-num_defaults]
        if 'self' in non_optional_args:
            non_optional_args.remove('self')
        for arg in func.__code__.co_varnames:
            if arg in default_args and arg not in kwargs:
                kwargs[arg] = default_args[arg]
        missing_args = list(set(non_optional_args) - set(kwargs))
        if missing_args:
            msg = "Argument {0} is required".format(missing_args)
            raise AirflowException(msg)

        kwargs['params'] = dag_params

        result = func(*args, **kwargs)
        return result
    return wrapper

if 'BUILDING_AIRFLOW_DOCS' in os.environ:
    # Monkey patch hook to get good function headers while building docs
    apply_defaults = lambda x: x

def ask_yesno(question):
    yes = set(['yes', 'y'])
    no = set(['no', 'n'])

    done = False
    print(question)
    while not done:
        choice = input().lower()
        if choice in yes:
            return True
        elif choice in no:
            return False
        else:
            print("Please respond by yes or no.")


def send_email(to, subject, html_content, files=None, dryrun=False):
    """
    Send email using backend specified in EMAIL_BACKEND.
    """
    path, attr = configuration.get('email', 'EMAIL_BACKEND').rsplit('.', 1)
    module = importlib.import_module(path)
    backend = getattr(module, attr)
    return backend(to, subject, html_content, files=files, dryrun=dryrun)


def send_email_smtp(to, subject, html_content, files=None, dryrun=False):
    """
    Send an email with html content

    >>> send_email('test@example.com', 'foo', '<b>Foo</b> bar', ['/dev/null'], dryrun=True)
    """
    SMTP_MAIL_FROM = configuration.get('smtp', 'SMTP_MAIL_FROM')

    if isinstance(to, basestring):
        if ',' in to:
            to = to.split(',')
        elif ';' in to:
            to = to.split(';')
        else:
            to = [to]

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = SMTP_MAIL_FROM
    msg['To'] = ", ".join(to)
    msg["Date"] = formatdate(localtime=True)
    mime_text = MIMEText(html_content, 'html')
    msg.attach(mime_text)

    for fname in files or []:
        basename = os.path.basename(fname)
        with open(fname, "rb") as f:
            msg.attach(MIMEApplication(
                f.read(),
                Content_Disposition='attachment; filename="%s"' % basename,
                Name=basename
            ))

    send_MIME_email(SMTP_MAIL_FROM, to, msg, dryrun)


def send_MIME_email(e_from, e_to, mime_msg, dryrun=False):
    SMTP_HOST = configuration.get('smtp', 'SMTP_HOST')
    SMTP_PORT = configuration.getint('smtp', 'SMTP_PORT')
    SMTP_USER = configuration.get('smtp', 'SMTP_USER')
    SMTP_PASSWORD = configuration.get('smtp', 'SMTP_PASSWORD')
    SMTP_STARTTLS = configuration.getboolean('smtp', 'SMTP_STARTTLS')
    SMTP_SSL = configuration.getboolean('smtp', 'SMTP_SSL')

    if not dryrun:
        s = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) if SMTP_SSL else smtplib.SMTP(SMTP_HOST, SMTP_PORT)
        if SMTP_STARTTLS:
            s.starttls()
        if SMTP_USER and SMTP_PASSWORD:
            s.login(SMTP_USER, SMTP_PASSWORD)
        logging.info("Sent an alert email to " + str(e_to))
        s.sendmail(e_from, e_to, mime_msg.as_string())
        s.quit()


def import_module_attrs(parent_module_globals, module_attrs_dict):
    '''
    Attempts to import a set of modules and specified attributes in the
    form of a dictionary. The attributes are copied in the parent module's
    namespace. The function returns a list of attributes names that can be
    affected to __all__.

    This is used in the context of ``operators`` and ``hooks`` and
    silence the import errors for when libraries are missing. It makes
    for a clean package abstracting the underlying modules and only
    brings functional operators to those namespaces.
    '''
    imported_attrs = []
    for mod, attrs in list(module_attrs_dict.items()):
        try:
            path = os.path.realpath(parent_module_globals['__file__'])
            folder = os.path.dirname(path)
            f, filename, description = imp.find_module(mod, [folder])
            module = imp.load_module(mod, f, filename, description)
            for attr in attrs:
                parent_module_globals[attr] = getattr(module, attr)
                imported_attrs += [attr]
        except Exception as err:
            logging.debug("Error importing module {mod}: {err}".format(
                mod=mod, err=err))
    return imported_attrs


def is_in(obj, l):
    """
    Checks whether an object is one of the item in the list.
    This is different from ``in`` because ``in`` uses __cmp__ when
    present. Here we change based on the object itself
    """
    for item in l:
        if item is obj:
            return True
    return False


@contextmanager
def TemporaryDirectory(suffix='', prefix=None, dir=None):
    name = mkdtemp(suffix=suffix, prefix=prefix, dir=dir)
    try:
        yield name
    finally:
            try:
                shutil.rmtree(name)
            except OSError as e:
                # ENOENT - no such file or directory
                if e.errno != errno.ENOENT:
                    raise e


class AirflowTaskTimeout(Exception):
    pass


class timeout(object):
    """
    To be used in a ``with`` block and timeout its content.
    """
    def __init__(self, seconds=1, error_message='Timeout'):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        logging.error("Process timed out")
        raise AirflowTaskTimeout(self.error_message)

    def __enter__(self):
        try:
            signal.signal(signal.SIGALRM, self.handle_timeout)
            signal.alarm(self.seconds)
        except ValueError as e:
            logging.warning("timeout can't be used in the current context")
            logging.exception(e)

    def __exit__(self, type, value, traceback):
        try:
            signal.alarm(0)
        except ValueError as e:
            logging.warning("timeout can't be used in the current context")
            logging.exception(e)


def is_container(obj):
    """
    Test if an object is a container (iterable) but not a string
    """
    return hasattr(obj, '__iter__') and not isinstance(obj, basestring)


def as_tuple(obj):
    """
    If obj is a container, returns obj as a tuple.
    Otherwise, returns a tuple containing obj.
    """
    if is_container(obj):
        return tuple(obj)
    else:
        return tuple([obj])


def round_time(dt, delta, start_date=datetime.min):
    """
    Returns the datetime of the form start_date + i * delta
    which is closest to dt for any non-negative integer i.

    Note that delta may be a datetime.timedelta or a dateutil.relativedelta

    >>> round_time(datetime(2015, 1, 1, 6), timedelta(days=1))
    datetime.datetime(2015, 1, 1, 0, 0)
    >>> round_time(datetime(2015, 1, 2), relativedelta(months=1))
    datetime.datetime(2015, 1, 1, 0, 0)
    >>> round_time(datetime(2015, 9, 16, 0, 0), timedelta(1), datetime(2015, 9, 14, 0, 0))
    datetime.datetime(2015, 9, 16, 0, 0)
    >>> round_time(datetime(2015, 9, 15, 0, 0), timedelta(1), datetime(2015, 9, 14, 0, 0))
    datetime.datetime(2015, 9, 15, 0, 0)
    >>> round_time(datetime(2015, 9, 14, 0, 0), timedelta(1), datetime(2015, 9, 14, 0, 0))
    datetime.datetime(2015, 9, 14, 0, 0)
    >>> round_time(datetime(2015, 9, 13, 0, 0), timedelta(1), datetime(2015, 9, 14, 0, 0))
    datetime.datetime(2015, 9, 14, 0, 0)
    """

    if isinstance(delta, six.string_types):
        # It's cron based, so it's easy
        cron = croniter(delta, start_date)
        prev = cron.get_prev(datetime)
        if prev == start_date:
            return start_date
        else:
            return prev

    # Ignore the microseconds of dt
    dt -= timedelta(microseconds = dt.microsecond)

    # We are looking for a datetime in the form start_date + i * delta
    # which is as close as possible to dt. Since delta could be a relative
    # delta we don't know it's exact length in seconds so we cannot rely on
    # division to find i. Instead we employ a binary search algorithm, first
    # finding an upper and lower limit and then disecting the interval until
    # we have found the closest match.

    # We first search an upper limit for i for which start_date + upper * delta
    # exceeds dt.
    upper = 1
    while start_date + upper*delta < dt:
        # To speed up finding an upper limit we grow this exponentially by a
        # factor of 2
        upper *= 2

    # Since upper is the first value for which start_date + upper * delta
    # exceeds dt, upper // 2 is below dt and therefore forms a lower limited
    # for the i we are looking for
    lower = upper // 2

    # We now continue to intersect the interval between
    # start_date + lower * delta and start_date + upper * delta
    # until we find the closest value
    while True:
        # Invariant: start + lower * delta < dt <= start + upper * delta
        # If start_date + (lower + 1)*delta exceeds dt, then either lower or
        # lower+1 has to be the solution we are searching for
        if start_date + (lower + 1)*delta >= dt:
            # Check if start_date + (lower + 1)*delta or
            # start_date + lower*delta is closer to dt and return the solution
            if (
                    (start_date + (lower + 1) * delta) - dt <=
                    dt - (start_date + lower * delta)):
                return start_date + (lower + 1)*delta
            else:
                return start_date + lower * delta

        # We intersect the interval and either replace the lower or upper
        # limit with the candidate
        candidate = lower + (upper - lower) // 2
        if start_date + candidate*delta >= dt:
            upper = candidate
        else:
            lower = candidate

    # in the special case when start_date > dt the search for upper will
    # immediately stop for upper == 1 which results in lower = upper // 2 = 0
    # and this function returns start_date.


def chain(*tasks):
    """
    Given a number of tasks, builds a dependency chain.

    chain(task_1, task_2, task_3, task_4)

    is equivalent to

    task_1.set_downstream(task_2)
    task_2.set_downstream(task_3)
    task_3.set_downstream(task_4)
    """
    for up_task, down_task in zip(tasks[:-1], tasks[1:]):
        up_task.set_downstream(down_task)


class AirflowJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        # convert dates and numpy objects in a json serializable format
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%dT%H:%M:%SZ')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        elif type(obj) in [np.int_, np.intc, np.intp, np.int8, np.int16,
                           np.int32, np.int64, np.uint8, np.uint16,
                           np.uint32, np.uint64]:
            return int(obj)
        elif type(obj) in [np.bool_]:
            return bool(obj)
        elif type(obj) in [np.float_, np.float16, np.float32, np.float64,
                           np.complex_, np.complex64, np.complex128]:
            return float(obj)

        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


class LoggingMixin(object):
    """
    Convenience super-class to have a logger configured with the class name
    """

    @property
    def logger(self):
        try:
            return self._logger
        except AttributeError:
            self._logger = logging.root.getChild(self.__class__.__module__ + '.' +self.__class__.__name__)
            return self._logger


class S3Log(object):
    """
    Utility class for reading and writing logs in S3.
    Requires airflow[s3] and setting the REMOTE_BASE_LOG_FOLDER and
    REMOTE_LOG_CONN_ID configuration options in airflow.cfg.
    """
    def __init__(self):
        remote_conn_id = configuration.get('core', 'REMOTE_LOG_CONN_ID')
        try:
            from airflow.hooks import S3Hook
            self.hook = S3Hook(remote_conn_id)
        except:
            self.hook = None
            logging.error(
                'Could not create an S3Hook with connection id "{}". '
                'Please make sure that airflow[s3] is installed and '
                'the S3 connection exists.'.format(remote_conn_id))

    def read(self, remote_log_location, return_error=False):
        """
        Returns the log found at the remote_log_location. Returns '' if no
        logs are found or there is an error.

        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: string (path)
        :param return_error: if True, returns a string error message if an
            error occurs. Otherwise returns '' when an error occurs.
        :type return_error: bool
        """
        if self.hook:
            try:
                s3_key = self.hook.get_key(remote_log_location)
                if s3_key:
                    return s3_key.get_contents_as_string().decode()
            except:
                pass

        # raise/return error if we get here
        err = 'Could not read logs from {}'.format(remote_log_location)
        logging.error(err)
        return err if return_error else ''


    def write(self, log, remote_log_location, append=False):
        """
        Writes the log to the remote_log_location. Fails silently if no hook
        was created.

        :param log: the log to write to the remote_log_location
        :type log: string
        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: string (path)
        :param append: if False, any existing log file is overwritten. If True,
            the new log is appended to any existing logs.
        :type append: bool

        """
        if self.hook:

            if append:
                old_log = self.read(remote_log_location)
                log = old_log + '\n' + log
            try:
                self.hook.load_string(
                    log,
                    key=remote_log_location,
                    replace=True,
                    encrypt=configuration.get('core', 'ENCRYPT_S3_LOGS'))
                return
            except:
                pass

        # raise/return error if we get here
        logging.error('Could not write logs to {}'.format(remote_log_location))


class GCSLog(object):
    """
    Utility class for reading and writing logs in GCS.
    Requires either airflow[gcloud] or airflow[gcp_api] and
    setting the REMOTE_BASE_LOG_FOLDER and REMOTE_LOG_CONN_ID configuration
    options in airflow.cfg.
    """
    def __init__(self):
        """
        Attempt to create hook with airflow[gcloud] (and set
        use_gcloud = True), otherwise uses airflow[gcp_api]
        """
        remote_conn_id = configuration.get('core', 'REMOTE_LOG_CONN_ID')
        self.use_gcloud = False

        try:
            from airflow.contrib.hooks import GCSHook
            self.hook = GCSHook(remote_conn_id)
            self.use_gcloud = True
        except:
            try:
                from airflow.contrib.hooks import GoogleCloudStorageHook
                self.hook = GoogleCloudStorageHook(remote_conn_id)
            except:
                self.hook = None
                logging.error(
                    'Could not create a GCSHook with connection id "{}". '
                    'Please make sure that either airflow[gcloud] or '
                    'airflow[gcp_api] is installed and the GCS connection '
                    'exists.'.format(remote_conn_id))

    def read(self, remote_log_location, return_error=True):
        """
        Returns the log found at the remote_log_location.

        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: string (path)
        :param return_error: if True, returns a string error message if an
            error occurs. Otherwise returns '' when an error occurs.
        :type return_error: bool
        """
        if self.hook:
            try:
                if self.use_gcloud:
                    gcs_blob = self.hook.get_blob(remote_log_location)
                    if gcs_blob:
                        return gcs_blob.download_as_string().decode()
                else:
                    bkt, blob = remote_log_location.lstrip('gs:/').split('/', 1)
                    return self.hook.download(bkt, blob).decode()
            except:
                pass

        # raise/return error if we get here
        err = 'Could not read logs from {}'.format(remote_log_location)
        logging.error(err)
        return err if return_error else ''

    def write(self, log, remote_log_location, append=False):
        """
        Writes the log to the remote_log_location. Fails silently if no hook
        was created.

        :param log: the log to write to the remote_log_location
        :type log: string
        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: string (path)
        :param append: if False, any existing log file is overwritten. If True,
            the new log is appended to any existing logs.
        :type append: bool

        """
        if self.hook:

            if append:
                old_log = self.read(remote_log_location)
                log = old_log + '\n' + log

            try:
                if self.use_gcloud:
                    self.hook.upload_from_string(
                        log,
                        blob=remote_log_location,
                        replace=True)
                    return
                else:
                    bkt, blob = remote_log_location.lstrip('gs:/').split('/', 1)
                    from tempfile import NamedTemporaryFile
                    with NamedTemporaryFile(mode='w+') as tmpfile:
                        tmpfile.write(log)
                        self.hook.upload(bkt, blob, tmpfile.name)
                    return
            except:
                pass

        # raise/return error if we get here
        logging.error('Could not write logs to {}'.format(remote_log_location))
