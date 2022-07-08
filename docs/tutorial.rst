
Airflow Tutorial
================

This tutorial walks through some of the fundamental concepts, objects
and their usage while writting your first pipeline.

Importing Modules
-----------------

An Airflow pipeline is just a common Python script that happens to define
an Airflow DAG object. Let's start by importing the libraries we might need.

.. code:: python

    # The DAG object, we'll need this to build a DAG
    from airflow import DAG

    # Operators, we need this to operate!
    from airflow.operators import BashOperator, MySqlOperator

Instantiate a DAG
-----------------

We'll need a DAG object to nest our tasks into. When calling the DAG class'
constructor, you need to pass a dag_id.

.. code:: python

    dag = DAG('tutorial')

Default Arguments
-----------------
We're about to create some tasks, and we have the choice to explicitely pass
a set of arguments to each task's constructor (which could become redundant), 
or (better!) we can define a dictionary of default parameters that we can use 
as default parameters when creating tasks.

.. code:: python

    from datetime import datetime

    args = {
        'owner': 'airflow',
        'depends_on_past': True,
        'start_date': datetime(2015, 01, 23),
        'email': ['airflow@airflow.com',],
        'email_on_failure': True,
        'email_on_retry': True,
    }

For more information about the BaseOperator's parameters and what they do,
refer to the :py:class:`airflow.models.BaseOperator` documentation.

Tasks
-----
Tasks are generated when instantiating objects from operators.

.. code:: python

    t1 = BashOperator(
        task_id='print_date', 
        bash_command='date', 
        default_args=args)

    t2 = BashOperator(
        task_id='sleep', 
        depends_on_past=False,
        bash_command='sleep 5', 
        default_args=args)

Notice how the default arguments we defined in the previous step are passed
to the operators constructor. This avoid from passing every argument for
every constructor call. Also, notice that in the second call we 
override ``depends_on_past`` parameter with False.

The precendence rules for operator is:

* Use the argument explicitely passed to the constructor
* Look in te default_args dictonary, use the value from there if it exists
* Use the operator's default, if any
* If none of these are defined, we raise an exception

Adding the tasks to the DAG
---------------------------
At this moment, the tasks have been created but have no relationship with the
DAG we created earlier. Here's how you add them:

.. code:: python

    dag.add_task(t1)
    dag.add_task(t2)

    # equivalent shortcut: 
    # dag.add_tasks([t1, t2])

Templating with Jinja
---------------------
Airflow leverages  
`Jinja Templating <http://jinja.pocoo.org/docs/dev/>`_  and gives 
the pipeline author
access to an array of builtin parameters and macros. Airflow also provides
hooks for the pipeline author to define their own parameters, macros and
templates.

The goal of this tutorial
around templating is letting you know it exists, getting familiar with
curly brackets, and point to the most common template variable: ``{{ ds }}``

.. code:: python

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

Notice the ``templated_command`` contains code logic in ``{% %}`` blocks,
parameters like ``{{ ds }}``, function calls like 
``{{ macros.ds_add(ds, 7)}}``, and references to user defined parameters
as in ``{{ params.my_param }}``.

The ``params`` hook in BaseOperator allows you to pass a dictionary of 
parameters and/or objects to your templates. Please take the time
to understand how the parameter ``my_param`` makes it through to the template.

Note that templated fields can point to actual files if you prefer. 
It may be desirable for many reasons, like keeping your scripts logic
outside of your pipeline code, getting proper code highlighting is files, 
and just generally keeping things where they belond. 

We could have 
had a file ``templated_command.sh``, and reference it in bash_command, as in
``bash_command='templated_command.sh'`` where the file location is relative
to the pipeline's (``tutorial.py``) location.

Setting up Dependencies
-----------------------
We have two simple tasks that do not depend on each other, here's a few ways
you can define dependencies between them:

.. code:: python

    t2.set_upstream(t1)

    # is equivalent to
    # t1.set_downstream(t2)

    t3.set_upstream(t1)

    # all of this is equivalent to 
    # dag.set_dependencies('print_date', 'sleep')
    # dag.set_dependencies('print_date', 'templated')

Note that when executing your script, Airflow will raise exceptions when
it finds cycles in your DAG, or when the same dependency is referenced more
than once.

Recap
-----
Alright, so we have a pretty basic DAG. At this point your code should look 
something like this:

.. code:: python
    
    from airflow import DAG
    from airflow.operators import BashOperator, MySqlOperator
    from datetime import datetime

    dag = DAG('tutorial')

    args = {
        'owner': 'airflow',
        'depends_on_past': True,
        'start_date': datetime(2015, 01, 23),
        'email': ['airflow@airflow.com',],
        'email_on_failure': True,
        'email_on_retry': True,
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

Testing
--------

Running the Script
''''''''''''''''''

Alright time to run some tests. First let's make sure that the pipeline
parses. Let's assume we're saving the code from the previous step in
``tutorial.py`` in the DAGs folder referenced in your ``airflow.cfg``.
The default location for your DAGs is ``~/airflow/dags``.

.. code-block:: bash

    python ~/airflow/dags/tutorial.py

If the script does not raise an exception it means that you haven't done
anything horribly wrong, and that your Airflow environment is somewhat
sound.

Command Line Metadata Validation
'''''''''''''''''''''''''''''''''
Let's run a few commands to validate this script further.

.. code-block:: bash

    # print the list of active DAGs
    airflow list_dags

    # prints the list of tasks the "tutorial" dag_id
    airflow list_tasks tutorial

    # prints the hierarchy of tasks in the tutorial DAG
    airflow list_tasks tutorial --tree


Testing
'''''''
Let's test by running the actual task instances on a specific date.

.. code-block:: bash

    # command layout: command subcommand dag_id task_id date

    # testing print_date
    airflow test tutorial print_date 2015-01-01

    # testing sleep
    airflow test tutorial sleep 2015-01-01

Now remember what we did with templating earlier? See how this template
gets rendered and executed by running this command:

.. code-block:: bash

    # testing templated
    airflow test tutorial templated 2015-01-01

This should result in displaying a verbose log of events and ultimately 
running your bash command and priting the result.

Note that the ``airflow test`` command runs task instances locally, output
their log to stdout (on screen), don't bother with dependencies, and
don't communicate their state (running, success, failed, ...) to the 
database. It simply allows to test a single a task instance.

Backfill
''''''''
Everything looks like it's running fine so let's run a backfill.
``backfill`` will respect your dependencies, log into files and talk to the
database to record status. If you do have a webserver up, you'll be able to
track the progress. ``airflow webserver`` will start a web server if you
are interested in tracking the progress visually as you backfill progresses.

.. code-block:: bash

    # optional, start a web server in debug mode in the background
    # airflow webserver --debug &

    # start your backfill on a date range
    airflow backfill tutorial -s 2015-01-01 -e 2015-01-07


What's Next?
-------------
That's it, you've written, tested and backfilled your very first Airflow
pipeline. Merging your code into a code repository that has a master scheduler
running on top of should get it to get triggered and run everyday.

Here's a few things you might want to do next:

* Take an in-depth tour of the UI, click all the things!
* Keep reading the docs! Especially the sections on:

    * Command line interface
    * Operators
    * Macros

* Write you first real pipeline
