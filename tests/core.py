from datetime import datetime, time
import unittest
from airflow import configuration
configuration.test_mode()
from airflow import jobs, models, DAG, executors, utils, operators
from airflow.www.app import app

NUM_EXAMPLE_DAGS = 3
DEV_NULL = '/dev/null'
LOCAL_EXECUTOR = executors.LocalExecutor()
DEFAULT_DATE = datetime(2015, 1, 1)
configuration.test_mode()


class HivePrestoTest(unittest.TestCase):

    def setUp(self):
        configuration.test_mode()
        utils.initdb()
        args = {'owner': 'airflow', 'start_date': datetime(2015, 1, 1)}
        dag = DAG('hive_test', default_args=args)
        self.dag = dag

    def test_hive(self):
        hql = """
        USE airflow;
        DROP TABLE IF EXISTS static_babynames_partitioned;
        CREATE TABLE IF NOT EXISTS static_babynames_partitioned (
            state string,
            year string,
            name string,
            gender string,
            num int)
        PARTITIONED BY (ds string);
        INSERT OVERWRITE TABLE static_babynames_partitioned
            PARTITION(ds='{{ ds }}')
        SELECT state, year, name, gender, num FROM static_babynames;
        """
        t = operators.HiveOperator(task_id='basic_hql', hql=hql, dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_presto(self):
        sql = """
        SELECT count(1) FROM airflow.static_babynames_partitioned;
        """
        t = operators.PrestoCheckOperator(
            task_id='presto_check', sql=sql, dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_hdfs_sensor(self):
        t = operators.HdfsSensor(
            task_id='hdfs_sensor_check',
            filepath='/user/hive/warehouse/airflow.db/static_babynames',
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_sql_sensor(self):
        t = operators.SqlSensor(
            task_id='hdfs_sensor_check',
            conn_id='presto_default',
            sql="SELECT 'x' FROM airflow.static_babynames LIMIT 1;",
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)


class CoreTest(unittest.TestCase):

    def setUp(self):
        configuration.test_mode()
        utils.initdb()
        self.dagbag = models.DagBag(
            dag_folder=DEV_NULL, include_examples=True)
        utils.initdb()
        args = {'owner': 'airflow', 'start_date': datetime(2015, 1, 1)}
        dag = DAG('core_test', default_args=args)
        self.dag = dag
        self.dag_bash = self.dagbag.dags['example_bash_operator']
        self.runme_0 = self.dag_bash.get_task('runme_0')

    def test_confirm_unittest_mod(self):
        assert configuration.conf.get('core', 'unit_test_mode')

    def test_time_sensor(self):
        t = operators.TimeSensor(
            task_id='time_sensor_check',
            target_time=time(0),
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_import_examples(self):
        self.assertEqual(len(self.dagbag.dags), NUM_EXAMPLE_DAGS)

    def test_local_task_job(self):
        TI = models.TaskInstance
        ti = TI(
            task=self.runme_0, execution_date=DEFAULT_DATE)
        job = jobs.LocalTaskJob(task_instance=ti, force=True)
        job.run()

    def test_master_job(self):
        job = jobs.MasterJob(dag_id='example_bash_operator', test_mode=True)
        job.run()

    def test_local_backfill_job(self):
        self.dag_bash.clear(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE)
        job = jobs.BackfillJob(
            dag=self.dag_bash,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE)
        job.run()

    def test_raw_job(self):
        TI = models.TaskInstance
        ti = TI(
            task=self.runme_0, execution_date=DEFAULT_DATE)
        ti.dag = self.dag_bash
        ti.run(force=True)


class WebUiTests(unittest.TestCase):

    def setUp(self):
        configuration.test_mode()
        utils.initdb()
        app.config['TESTING'] = True
        self.app = app.test_client()

    def test_index(self):
        response = self.app.get('/', follow_redirects=True)
        assert "DAGs" in response.data
        assert "example_bash_operator" in response.data

    def test_query(self):
        response = self.app.get('/admin/airflow/query')
        assert "Ad Hoc Query" in response.data
        response = self.app.get(
            "/admin/airflow/query?"
            "conn_id=presto_default&"
            "sql=SELECT+COUNT%281%29+FROM+airflow.static_babynames")
        assert "Ad Hoc Query" in response.data

    def test_health(self):
        response = self.app.get('/health')
        assert 'The server is healthy!' in response.data

    def test_dag_views(self):
        response = self.app.get(
            '/admin/airflow/graph?dag_id=example_bash_operator')
        assert "runme_0" in response.data
        response = self.app.get(
            '/admin/airflow/tree?num_runs=25&dag_id=example_bash_operator')
        assert "runme_0" in response.data
        response = self.app.get(
            '/admin/airflow/duration?days=30&dag_id=example_bash_operator')
        assert "DAG: example_bash_operator" in response.data
        response = self.app.get(
            '/admin/airflow/landing_times?'
            'days=30&dag_id=example_bash_operator')
        assert "DAG: example_bash_operator" in response.data
        response = self.app.get(
            '/admin/airflow/gantt?dag_id=example_bash_operator')
        assert "DAG: example_bash_operator" in response.data
        response = self.app.get(
            '/admin/airflow/code?dag_id=example_bash_operator')
        assert "DAG: example_bash_operator" in response.data
        response = self.app.get(
            '/admin/airflow/conf')
        assert "Airflow Configuration" in response.data
        response = self.app.get(
            '/admin/airflow/rendered?'
            'task_id=runme_1&dag_id=example_bash_operator&'
            'execution_date=2015-01-07T00:00:00')
        assert "example_bash_operator__runme_1__20150107" in response.data
        response = self.app.get(
            '/admin/airflow/log?task_id=run_this_last&'
            'dag_id=example_bash_operator&execution_date=2015-01-01T00:00:00')
        assert "Logs for run_this_last on 2015-01-01T00:00:00" in response.data
        response = self.app.get(
            '/admin/airflow/task?task_id=runme_0&dag_id=example_bash_operator')
        assert "Attributes" in response.data
        response = self.app.get(
            '/admin/airflow/dag_stats')
        assert "example_bash_operator" in response.data
        response = self.app.get(
            '/admin/airflow/action?action=clear&task_id=run_this_last&'
            'dag_id=example_bash_operator&future=true&past=false&'
            'upstream=true&downstream=false&'
            'execution_date=2015-01-01T00:00:00&'
            'origin=http%3A%2F%2Fjn8.brain.musta.ch%3A8080%2Fadmin%2Fairflow'
            '%2Ftree%3Fnum_runs%3D65%26dag_id%3Dexample_bash_operator')
        assert "Wait a minute" in response.data
        response = self.app.get(
            '/admin/airflow/action?action=clear&task_id=run_this_last&'
            'dag_id=example_bash_operator&future=true&past=false&'
            'upstream=true&downstream=false&'
            'execution_date=2015-01-01T00:00:00&confirmed=true&'
            'origin=http%3A%2F%2Fjn8.brain.musta.ch%3A8080%2Fadmin%2Fairflow'
            '%2Ftree%3Fnum_runs%3D65%26dag_id%3Dexample_bash_operator')

    def test_charts(self):
        response = self.app.get(
            '/admin/airflow/chart?chart_id=1&iteration_no=1')
        assert "Most Popular" in response.data
        response = self.app.get(
            '/admin/airflow/chart_data?chart_id=1&iteration_no=1')
        assert "Michael" in response.data

    def tearDown(self):
        pass


if __name__ == '__main__':
        unittest.main()
