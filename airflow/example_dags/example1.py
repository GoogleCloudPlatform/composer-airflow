from airflow.operators import BashOperator, DummyOperator
from airflow.models import DAG
from datetime import datetime

default_args = {
    'owner': 'max',
    'start_date': datetime(2014, 11, 1),
}

dag = DAG(dag_id='example_1')
# dag = DAG(dag_id='example_1', executor=SequentialExecutor())

cmd = 'ls -l'
run_this_last = DummyOperator(
    task_id='run_this_last',
    default_args=default_args)
dag.add_task(run_this_last)

run_this = BashOperator(
    task_id='run_after_loop', bash_command='echo 1',
    default_args=default_args)
dag.add_task(run_this)
run_this.set_downstream(run_this_last)
for i in range(9):
    i = str(i)
    task = BashOperator(
        task_id='runme_'+i,
        bash_command='sleep 5',
        default_args=default_args)
    task.set_downstream(run_this)
    dag.add_task(task)

task = BashOperator(
    task_id='also_run_this',
    bash_command='ls -l',
    default_args=default_args)
dag.add_task(task)
task.set_downstream(run_this_last)
