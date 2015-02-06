"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime

dag = DAG('tutorial')

args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2015, 01, 23),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    default_args=args)

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    default_args=args)

templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

t3 = BashOperator(
    task_id='templated',
    depends_on_past=False,
    bash_command=templated_command,
    params={'my_param': 'Paramater I passed in'},
    default_args=args)

t2.set_upstream(t1)
t3.set_upstream(t1)

dag.add_tasks([t1, t2, t3])
