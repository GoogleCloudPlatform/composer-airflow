#!python

from flux import settings
from flux.models import DagBag, TaskInstance, DagPickle, State

import dateutil.parser
from datetime import datetime
import logging
import os
import pickle
import signal
import sys
from time import sleep

import argparse
from sqlalchemy import func

# Common help text across subcommands
mark_success_help = "Mark jobs as succeeded without running them"
subdir_help = "File location or directory from which to look for the dag"


def backfill(args):
    logging.basicConfig(level=logging.INFO)
    dagbag = DagBag(args.subdir)
    if args.dag_id not in dagbag.dags:
        raise Exception('dag_id could not be found')
    dag = dagbag.dags[args.dag_id]

    if args.start_date:
        args.start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    if args.end_date:
        args.end_date = datetime.strptime(args.end_date, '%Y-%m-%d')

    if args.task_regex:
        dag = dag.sub_dag(task_regex=args.task_regex)

    dag.run(
        start_date=args.start_date,
        end_date=args.end_date,
        mark_success=args.mark_success)


def run(args):

    # Setting up logging
    directory = settings.BASE_LOG_FOLDER + \
        "/{args.dag_id}/{args.task_id}".format(args=args)
    if not os.path.exists(directory):
        os.makedirs(directory)
    args.execution_date = dateutil.parser.parse(args.execution_date)
    iso = args.execution_date.isoformat()
    filename = "{directory}/{iso}".format(**locals())
    logging.basicConfig(
        filename=filename, level=logging.INFO, format=settings.LOG_FORMAT)
    print("Logging into: " + filename)

    if not args.pickle:
        dagbag = DagBag(args.subdir)
        if args.dag_id not in dagbag.dags:
            raise Exception('dag_id could not be found')
        dag = dagbag.dags[args.dag_id]
        task = dag.get_task(task_id=args.task_id)
    else:
        session = settings.Session()
        dag_pickle, = session.query(
            DagPickle).filter(DagPickle.id==args.pickle).all()
        dag = dag_pickle.get_object()
        task = dag.get_task(task_id=args.task_id)
        
    ti = TaskInstance(task, args.execution_date)

    # This is enough to fail the task instance
    def signal_handler(signum, frame):
        logging.error("SIGINT (ctrl-c) received".format(args.task_id))
        ti.error(args.execution_date)
        sys.exit()
    signal.signal(signal.SIGINT, signal_handler)

    ti.run(
        mark_success=args.mark_success,
        force=args.force,
        ignore_dependencies=args.ignore_dependencies)


def clear(args):
    logging.basicConfig(level=logging.INFO)
    dagbag = DagBag(args.subdir)
    if args.dag_id not in dagbag.dags:
        raise Exception('dag_id could not be found')
    dag = dagbag.dags[args.dag_id]

    if args.start_date:
        args.start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    if args.end_date:
        args.end_date = datetime.strptime(args.end_date, '%Y-%m-%d')

    if args.task_regex:
        dag = dag.sub_dag(task_regex=args.task_regex)
    dag.clear(
        start_date=args.start_date, end_date=args.end_date)


def webserver(args):
    logging.basicConfig(level=logging.DEBUG, format=settings.LOG_FORMAT)
    print(settings.HEADER)
    from www.app import app
    print("Starting the web server on port {0}.".format(args.port))
    app.run(debug=True, port=args.port)


def master(args):

    logging.basicConfig(level=logging.DEBUG)
    logging.info("Starting a master scheduler")

    session = settings.Session()
    TI = TaskInstance
    # This should get new code
    dagbag = DagBag(args.subdir)
    executor = dagbag.executor()
    executor.start()
    if args.dag_id:
        dags = [dagbag.dags[args.dag_id]]
    else:
        dags = dagbag.dags.values()
    while True:
        for dag in dags:
            logging.info(
                "Getting latest instance for all task in dag " + dag.dag_id)
            sq = session.query(
                TI.task_id,
                func.max(TI.execution_date).label('max_ti')
            ).filter(
                TI.dag_id == dag.dag_id).group_by(TI.task_id).subquery('sq')
            qry = session.query(TI).filter(
                TI.dag_id == dag.dag_id,
                TI.task_id == sq.c.task_id,
                TI.execution_date == sq.c.max_ti,
            )
            latest_ti = qry.all()
            ti_dict = {ti.task_id: ti for ti in latest_ti}
            session.commit()

            for task in dag.tasks:
                if task.task_id not in ti_dict:
                    # Brand new task, let's get started
                    ti = TI(task, task.start_date)
                    executor.queue_command(ti.key, ti.command())
                else:
                    ti = ti_dict[task.task_id]
                    ti.task = task  # Hacky but worky
                    if ti.state == State.UP_FOR_RETRY:
                        # If task instance if up for retry, make sure
                        # the retry delay is met
                        if ti.is_runnable():
                            executor.queue_command(
                                ti.key, ti.command())
                    elif ti.state == State.RUNNING:
                        # Only one task at a time
                        continue
                    else:
                        # Trying to run the next schedule
                        ti = TI(
                            task=task,
                            execution_date=ti.execution_date +
                            task.schedule_interval
                        )
                        ti.refresh_from_db()
                        if ti.is_runnable():
                            executor.queue_command(ti.key, ti.command())
            session.close()
        sleep(5)
    executor.end()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help='sub-command help')

    ht = "Run subsections of a DAG for a specified date range"
    parser_backfill = subparsers.add_parser('backfill', help=ht)
    parser_backfill.add_argument("dag_id", help="The id of the dag to run")
    parser_backfill.add_argument(
        "-t", "--task_regex",
        help="The regex to filter specific task_ids to backfill (optional)")
    parser_backfill.add_argument(
        "-s", "--start_date", help="Overide start_date YYYY-MM-DD")
    parser_backfill.add_argument(
        "-e", "--end_date", help="Overide end_date YYYY-MM-DD")
    parser_backfill.add_argument(
        "-m", "--mark_success",
        help=mark_success_help, action="store_true")
    parser_backfill.add_argument(
        "-sd", "--subdir", help=subdir_help,
        default=settings.DAGS_FOLDER)
    parser_backfill.set_defaults(func=backfill)

    ht = "Clear a set of task instance, as if they never ran"
    parser_clear = subparsers.add_parser('clear', help=ht)
    parser_clear.add_argument("dag_id", help="The id of the dag to run")
    parser_clear.add_argument(
        "-t", "--task_regex",
        help="The regex to filter specific task_ids to clear (optional)")
    parser_clear.add_argument(
        "-s", "--start_date", help="Overide start_date YYYY-MM-DD")
    parser_clear.add_argument(
        "-e", "--end_date", help="Overide end_date YYYY-MM-DD")
    parser_clear.add_argument(
        "-sd", "--subdir", help=subdir_help,
        default=settings.DAGS_FOLDER)
    parser_clear.set_defaults(func=clear)

    ht = "Run a single task instance"
    parser_run = subparsers.add_parser('run', help=ht)
    parser_run.add_argument("dag_id", help="The id of the dag to run")
    parser_run.add_argument("task_id", help="The task_id to run")
    parser_run.add_argument(
        "execution_date", help="The execution date to run")
    parser_run.add_argument(
        "-sd", "--subdir", help=subdir_help, default=settings.DAGS_FOLDER)
    parser_run.add_argument(
        "-m", "--mark_success", help=mark_success_help, action="store_true")
    parser_run.set_defaults(func=run)
    ht = "Force a run regardless or previous success"
    parser_run.add_argument(
        "-f", "--force", help=ht, action="store_true")
    ht = "Ignore upstream and depends_on_past dependencies"
    parser_run.add_argument(
        "-i", "--ignore_dependencies", help=ht, action="store_true")
    ht = "Serialized pickle object of the entire dag (used internally)"
    parser_run.add_argument(
        "-p", "--pickle", help=ht)

    ht = "Start a Flux webserver instance"
    parser_webserver = subparsers.add_parser('webserver', help=ht)
    parser_webserver.add_argument(
        "-p", "--port",
        default=8080,
        type=int,
        help="Set the port on which to run the web server")
    parser_webserver.set_defaults(func=webserver)

    ht = "Start a master scheduler instance"
    parser_master = subparsers.add_parser('master', help=ht)
    parser_master.add_argument(
        "-d", "--dag_id", help="The id of the dag to run")
    parser_master.add_argument(
        "-sd", "--subdir", help=subdir_help, default=settings.DAGS_FOLDER)
    parser_master.set_defaults(func=master)

    args = parser.parse_args()
    args.func(args)
