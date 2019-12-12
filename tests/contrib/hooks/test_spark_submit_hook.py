# -*- coding: utf-8 -*-
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
import six
import unittest

from airflow import AirflowException
from airflow.models import Connection
from airflow.utils import db
from mock import patch, call

from airflow.contrib.hooks.spark_submit_hook import SparkSubmitHook


class TestSparkSubmitHook(unittest.TestCase):

    _spark_job_file = 'test_application.py'
    _config = {
        'conf': {
            'parquet.compression': 'SNAPPY'
        },
        'conn_id': 'default_spark',
        'files': 'hive-site.xml',
        'py_files': 'sample_library.py',
        'archives': 'sample_archive.zip#SAMPLE',
        'jars': 'parquet.jar',
        'packages': 'com.databricks:spark-avro_2.11:3.2.0',
        'exclude_packages': 'org.bad.dependency:1.0.0',
        'repositories': 'http://myrepo.org',
        'total_executor_cores': 4,
        'executor_cores': 4,
        'executor_memory': '22g',
        'keytab': 'privileged_user.keytab',
        'principal': 'user/spark@airflow.org',
        'proxy_user': 'sample_user',
        'name': 'spark-job',
        'num_executors': 10,
        'verbose': True,
        'driver_memory': '3g',
        'java_class': 'com.foo.bar.AppMain',
        'application_args': [
            '-f', 'foo',
            '--bar', 'bar',
            '--with-spaces', 'args should keep embdedded spaces',
            'baz'
        ]
    }

    @staticmethod
    def cmd_args_to_dict(list_cmd):
        return_dict = {}
        for arg in list_cmd:
            if arg.startswith("--"):
                pos = list_cmd.index(arg)
                return_dict[arg] = list_cmd[pos + 1]
        return return_dict

    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id='spark_yarn_cluster', conn_type='spark',
                host='yarn://yarn-master',
                extra='{"queue": "root.etl", "deploy-mode": "cluster"}')
        )
        db.merge_conn(
            Connection(
                conn_id='spark_k8s_cluster', conn_type='spark',
                host='k8s://https://k8s-master',
                extra='{"spark-home": "/opt/spark", ' +
                      '"deploy-mode": "cluster", ' +
                      '"namespace": "mynamespace"}')
        )
        db.merge_conn(
            Connection(
                conn_id='spark_default_mesos', conn_type='spark',
                host='mesos://host', port=5050)
        )

        db.merge_conn(
            Connection(
                conn_id='spark_home_set', conn_type='spark',
                host='yarn://yarn-master',
                extra='{"spark-home": "/opt/myspark"}')
        )

        db.merge_conn(
            Connection(
                conn_id='spark_home_not_set', conn_type='spark',
                host='yarn://yarn-master')
        )
        db.merge_conn(
            Connection(
                conn_id='spark_binary_set', conn_type='spark',
                host='yarn', extra='{"spark-binary": "custom-spark-submit"}')
        )
        db.merge_conn(
            Connection(
                conn_id='spark_binary_and_home_set', conn_type='spark',
                host='yarn',
                extra='{"spark-home": "/path/to/spark_home", ' +
                      '"spark-binary": "custom-spark-submit"}')
        )
        db.merge_conn(
            Connection(
                conn_id='spark_standalone_cluster', conn_type='spark',
                host='spark://spark-standalone-master:6066',
                extra='{"spark-home": "/path/to/spark_home", "deploy-mode": "cluster"}')
        )
        db.merge_conn(
            Connection(
                conn_id='spark_standalone_cluster_client_mode', conn_type='spark',
                host='spark://spark-standalone-master:6066',
                extra='{"spark-home": "/path/to/spark_home", "deploy-mode": "client"}')
        )

    def test_build_spark_submit_command(self):
        # Given
        hook = SparkSubmitHook(**self._config)

        # When
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        expected_build_cmd = [
            'spark-submit',
            '--master', 'yarn',
            '--conf', 'parquet.compression=SNAPPY',
            '--files', 'hive-site.xml',
            '--py-files', 'sample_library.py',
            '--archives', 'sample_archive.zip#SAMPLE',
            '--jars', 'parquet.jar',
            '--packages', 'com.databricks:spark-avro_2.11:3.2.0',
            '--exclude-packages', 'org.bad.dependency:1.0.0',
            '--repositories', 'http://myrepo.org',
            '--num-executors', '10',
            '--total-executor-cores', '4',
            '--executor-cores', '4',
            '--executor-memory', '22g',
            '--driver-memory', '3g',
            '--keytab', 'privileged_user.keytab',
            '--principal', 'user/spark@airflow.org',
            '--proxy-user', 'sample_user',
            '--name', 'spark-job',
            '--class', 'com.foo.bar.AppMain',
            '--verbose',
            'test_application.py',
            '-f', 'foo',
            '--bar', 'bar',
            '--with-spaces', 'args should keep embdedded spaces',
            'baz'
        ]
        self.assertEqual(expected_build_cmd, cmd)

    @patch('airflow.contrib.hooks.spark_submit_hook.subprocess.Popen')
    def test_spark_process_runcmd(self, mock_popen):
        # Given
        mock_popen.return_value.stdout = six.StringIO('stdout')
        mock_popen.return_value.stderr = six.StringIO('stderr')
        mock_popen.return_value.wait.return_value = 0

        # When
        hook = SparkSubmitHook(conn_id='')
        hook.submit()

        # Then
        self.assertEqual(mock_popen.mock_calls[0],
                         call(['spark-submit', '--master', 'yarn',
                               '--name', 'default-name', ''],
                              stderr=-2, stdout=-1, universal_newlines=True, bufsize=-1))

    def test_resolve_should_track_driver_status(self):
        # Given
        hook_default = SparkSubmitHook(conn_id='')
        hook_spark_yarn_cluster = SparkSubmitHook(conn_id='spark_yarn_cluster')
        hook_spark_k8s_cluster = SparkSubmitHook(conn_id='spark_k8s_cluster')
        hook_spark_default_mesos = SparkSubmitHook(conn_id='spark_default_mesos')
        hook_spark_home_set = SparkSubmitHook(conn_id='spark_home_set')
        hook_spark_home_not_set = SparkSubmitHook(conn_id='spark_home_not_set')
        hook_spark_binary_set = SparkSubmitHook(conn_id='spark_binary_set')
        hook_spark_binary_and_home_set = SparkSubmitHook(
            conn_id='spark_binary_and_home_set')
        hook_spark_standalone_cluster = SparkSubmitHook(
            conn_id='spark_standalone_cluster')

        # When
        should_track_driver_status_default = hook_default \
            ._resolve_should_track_driver_status()
        should_track_driver_status_spark_yarn_cluster = hook_spark_yarn_cluster \
            ._resolve_should_track_driver_status()
        should_track_driver_status_spark_k8s_cluster = hook_spark_k8s_cluster \
            ._resolve_should_track_driver_status()
        should_track_driver_status_spark_default_mesos = hook_spark_default_mesos \
            ._resolve_should_track_driver_status()
        should_track_driver_status_spark_home_set = hook_spark_home_set \
            ._resolve_should_track_driver_status()
        should_track_driver_status_spark_home_not_set = hook_spark_home_not_set \
            ._resolve_should_track_driver_status()
        should_track_driver_status_spark_binary_set = hook_spark_binary_set \
            ._resolve_should_track_driver_status()
        should_track_driver_status_spark_binary_and_home_set = \
            hook_spark_binary_and_home_set._resolve_should_track_driver_status()
        should_track_driver_status_spark_standalone_cluster = \
            hook_spark_standalone_cluster._resolve_should_track_driver_status()

        # Then
        self.assertEqual(should_track_driver_status_default, False)
        self.assertEqual(should_track_driver_status_spark_yarn_cluster, False)
        self.assertEqual(should_track_driver_status_spark_k8s_cluster, False)
        self.assertEqual(should_track_driver_status_spark_default_mesos, False)
        self.assertEqual(should_track_driver_status_spark_home_set, False)
        self.assertEqual(should_track_driver_status_spark_home_not_set, False)
        self.assertEqual(should_track_driver_status_spark_binary_set, False)
        self.assertEqual(should_track_driver_status_spark_binary_and_home_set, False)
        self.assertEqual(should_track_driver_status_spark_standalone_cluster, True)

    def test_resolve_connection_yarn_default(self):
        # Given
        hook = SparkSubmitHook(conn_id='')

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        dict_cmd = self.cmd_args_to_dict(cmd)
        expected_spark_connection = {"master": "yarn",
                                     "spark_binary": "spark-submit",
                                     "deploy_mode": None,
                                     "queue": None,
                                     "spark_home": None,
                                     "namespace": None}
        self.assertEqual(connection, expected_spark_connection)
        self.assertEqual(dict_cmd["--master"], "yarn")

    def test_resolve_connection_yarn_default_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_default')

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        dict_cmd = self.cmd_args_to_dict(cmd)
        expected_spark_connection = {"master": "yarn",
                                     "spark_binary": "spark-submit",
                                     "deploy_mode": None,
                                     "queue": "root.default",
                                     "spark_home": None,
                                     "namespace": None}
        self.assertEqual(connection, expected_spark_connection)
        self.assertEqual(dict_cmd["--master"], "yarn")
        self.assertEqual(dict_cmd["--queue"], "root.default")

    def test_resolve_connection_mesos_default_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_default_mesos')

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        dict_cmd = self.cmd_args_to_dict(cmd)
        expected_spark_connection = {"master": "mesos://host:5050",
                                     "spark_binary": "spark-submit",
                                     "deploy_mode": None,
                                     "queue": None,
                                     "spark_home": None,
                                     "namespace": None}
        self.assertEqual(connection, expected_spark_connection)
        self.assertEqual(dict_cmd["--master"], "mesos://host:5050")

    def test_resolve_connection_spark_yarn_cluster_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_yarn_cluster')

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        dict_cmd = self.cmd_args_to_dict(cmd)
        expected_spark_connection = {"master": "yarn://yarn-master",
                                     "spark_binary": "spark-submit",
                                     "deploy_mode": "cluster",
                                     "queue": "root.etl",
                                     "spark_home": None,
                                     "namespace": None}
        self.assertEqual(connection, expected_spark_connection)
        self.assertEqual(dict_cmd["--master"], "yarn://yarn-master")
        self.assertEqual(dict_cmd["--queue"], "root.etl")
        self.assertEqual(dict_cmd["--deploy-mode"], "cluster")

    def test_resolve_connection_spark_k8s_cluster_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_k8s_cluster')

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        dict_cmd = self.cmd_args_to_dict(cmd)
        expected_spark_connection = {"spark_home": "/opt/spark",
                                     "queue": None,
                                     "spark_binary": "spark-submit",
                                     "master": "k8s://https://k8s-master",
                                     "deploy_mode": "cluster",
                                     "namespace": "mynamespace"}
        self.assertEqual(connection, expected_spark_connection)
        self.assertEqual(dict_cmd["--master"], "k8s://https://k8s-master")
        self.assertEqual(dict_cmd["--deploy-mode"], "cluster")

    def test_resolve_connection_spark_home_set_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_home_set')

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        expected_spark_connection = {"master": "yarn://yarn-master",
                                     "spark_binary": "spark-submit",
                                     "deploy_mode": None,
                                     "queue": None,
                                     "spark_home": "/opt/myspark",
                                     "namespace": None}
        self.assertEqual(connection, expected_spark_connection)
        self.assertEqual(cmd[0], '/opt/myspark/bin/spark-submit')

    def test_resolve_connection_spark_home_not_set_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_home_not_set')

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        expected_spark_connection = {"master": "yarn://yarn-master",
                                     "spark_binary": "spark-submit",
                                     "deploy_mode": None,
                                     "queue": None,
                                     "spark_home": None,
                                     "namespace": None}
        self.assertEqual(connection, expected_spark_connection)
        self.assertEqual(cmd[0], 'spark-submit')

    def test_resolve_connection_spark_binary_set_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_binary_set')

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        expected_spark_connection = {"master": "yarn",
                                     "spark_binary": "custom-spark-submit",
                                     "deploy_mode": None,
                                     "queue": None,
                                     "spark_home": None,
                                     "namespace": None}
        self.assertEqual(connection, expected_spark_connection)
        self.assertEqual(cmd[0], 'custom-spark-submit')

    def test_resolve_connection_spark_binary_default_value_override(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_binary_set',
                               spark_binary='another-custom-spark-submit')

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        expected_spark_connection = {"master": "yarn",
                                     "spark_binary": "another-custom-spark-submit",
                                     "deploy_mode": None,
                                     "queue": None,
                                     "spark_home": None,
                                     "namespace": None}
        self.assertEqual(connection, expected_spark_connection)
        self.assertEqual(cmd[0], 'another-custom-spark-submit')

    def test_resolve_connection_spark_binary_default_value(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_default')

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        expected_spark_connection = {"master": "yarn",
                                     "spark_binary": "spark-submit",
                                     "deploy_mode": None,
                                     "queue": 'root.default',
                                     "spark_home": None,
                                     "namespace": None}
        self.assertEqual(connection, expected_spark_connection)
        self.assertEqual(cmd[0], 'spark-submit')

    def test_resolve_connection_spark_binary_and_home_set_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_binary_and_home_set')

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        expected_spark_connection = {"master": "yarn",
                                     "spark_binary": "custom-spark-submit",
                                     "deploy_mode": None,
                                     "queue": None,
                                     "spark_home": "/path/to/spark_home",
                                     "namespace": None}
        self.assertEqual(connection, expected_spark_connection)
        self.assertEqual(cmd[0], '/path/to/spark_home/bin/custom-spark-submit')

    def test_resolve_connection_spark_standalone_cluster_connection(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_standalone_cluster')

        # When
        connection = hook._resolve_connection()
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        expected_spark_connection = {"master": "spark://spark-standalone-master:6066",
                                     "spark_binary": "spark-submit",
                                     "deploy_mode": "cluster",
                                     "queue": None,
                                     "spark_home": "/path/to/spark_home",
                                     "namespace": None}
        self.assertEqual(connection, expected_spark_connection)
        self.assertEqual(cmd[0], '/path/to/spark_home/bin/spark-submit')

    def test_resolve_spark_submit_env_vars_standalone_client_mode(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_standalone_cluster_client_mode',
                               env_vars={"bar": "foo"})

        # When
        hook._build_spark_submit_command(self._spark_job_file)

        # Then
        self.assertEqual(hook._env, {"bar": "foo"})

    def test_resolve_spark_submit_env_vars_standalone_cluster_mode(self):

        def env_vars_exception_in_standalone_cluster_mode():
            # Given
            hook = SparkSubmitHook(conn_id='spark_standalone_cluster',
                                   env_vars={"bar": "foo"})

            # When
            hook._build_spark_submit_command(self._spark_job_file)

        # Then
        self.assertRaises(AirflowException,
                          env_vars_exception_in_standalone_cluster_mode)

    def test_resolve_spark_submit_env_vars_yarn(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_yarn_cluster',
                               env_vars={"bar": "foo"})

        # When
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        self.assertEqual(cmd[4], "spark.yarn.appMasterEnv.bar=foo")
        self.assertEqual(hook._env, {"bar": "foo"})

    def test_resolve_spark_submit_env_vars_k8s(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_k8s_cluster',
                               env_vars={"bar": "foo"})

        # When
        cmd = hook._build_spark_submit_command(self._spark_job_file)

        # Then
        self.assertEqual(cmd[4], "spark.kubernetes.driverEnv.bar=foo")

    def test_process_spark_submit_log_yarn(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_yarn_cluster')
        log_lines = [
            'SPARK_MAJOR_VERSION is set to 2, using Spark2',
            'WARN NativeCodeLoader: Unable to load native-hadoop library for your ' +
            'platform... using builtin-java classes where applicable',
            'WARN DomainSocketFactory: The short-circuit local reads feature cannot '
            'be used because libhadoop cannot be loaded.',
            'INFO Client: Requesting a new application from cluster with 10 NodeManagers',
            'INFO Client: Submitting application application_1486558679801_1820 ' +
            'to ResourceManager'
        ]
        # When
        hook._process_spark_submit_log(log_lines)

        # Then

        self.assertEqual(hook._yarn_application_id, 'application_1486558679801_1820')

    def test_process_spark_submit_log_k8s(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_k8s_cluster')
        log_lines = [
            'INFO  LoggingPodStatusWatcherImpl:54 - State changed, new state:' +
            'pod name: spark-pi-edf2ace37be7353a958b38733a12f8e6-driver' +
            'namespace: default' +
            'labels: spark-app-selector -> spark-465b868ada474bda82ccb84ab2747fcd,' +
            'spark-role -> driver' +
            'pod uid: ba9c61f6-205f-11e8-b65f-d48564c88e42' +
            'creation time: 2018-03-05T10:26:55Z' +
            'service account name: spark' +
            'volumes: spark-init-properties, download-jars-volume,' +
            'download-files-volume, spark-token-2vmlm' +
            'node name: N/A' +
            'start time: N/A' +
            'container images: N/A' +
            'phase: Pending' +
            'status: []' +
            '2018-03-05 11:26:56 INFO  LoggingPodStatusWatcherImpl:54 - State changed,' +
            ' new state:' +
            'pod name: spark-pi-edf2ace37be7353a958b38733a12f8e6-driver' +
            'namespace: default' +
            'Exit code: 999'
        ]

        # When
        hook._process_spark_submit_log(log_lines)

        # Then
        self.assertEqual(hook._kubernetes_driver_pod,
                         'spark-pi-edf2ace37be7353a958b38733a12f8e6-driver')
        self.assertEqual(hook._spark_exit_code, 999)

    def test_process_spark_submit_log_standalone_cluster(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_standalone_cluster')
        log_lines = [
            'Running Spark using the REST application submission protocol.',
            '17/11/28 11:14:15 INFO RestSubmissionClient: Submitting a request '
            'to launch an application in spark://spark-standalone-master:6066',
            '17/11/28 11:14:15 INFO RestSubmissionClient: Submission successfully ' +
            'created as driver-20171128111415-0001. Polling submission state...'
        ]
        # When
        hook._process_spark_submit_log(log_lines)

        # Then

        self.assertEqual(hook._driver_id, 'driver-20171128111415-0001')

    def test_process_spark_driver_status_log(self):
        # Given
        hook = SparkSubmitHook(conn_id='spark_standalone_cluster')
        log_lines = [
            'Submitting a request for the status of submission ' +
            'driver-20171128111415-0001 in spark://spark-standalone-master:6066',
            '17/11/28 11:15:37 INFO RestSubmissionClient: Server responded with ' +
            'SubmissionStatusResponse:',
            '{',
            '"action" : "SubmissionStatusResponse",',
            '"driverState" : "RUNNING",',
            '"serverSparkVersion" : "1.6.0",',
            '"submissionId" : "driver-20171128111415-0001",',
            '"success" : true,',
            '"workerHostPort" : "172.18.0.7:38561",',
            '"workerId" : "worker-20171128110741-172.18.0.7-38561"',
            '}'
        ]
        # When
        hook._process_spark_status_log(log_lines)

        # Then

        self.assertEqual(hook._driver_status, 'RUNNING')

    @patch('airflow.contrib.hooks.spark_submit_hook.subprocess.Popen')
    def test_yarn_process_on_kill(self, mock_popen):
        # Given
        mock_popen.return_value.stdout = six.StringIO('stdout')
        mock_popen.return_value.stderr = six.StringIO('stderr')
        mock_popen.return_value.poll.return_value = None
        mock_popen.return_value.wait.return_value = 0
        log_lines = [
            'SPARK_MAJOR_VERSION is set to 2, using Spark2',
            'WARN NativeCodeLoader: Unable to load native-hadoop library for your ' +
            'platform... using builtin-java classes where applicable',
            'WARN DomainSocketFactory: The short-circuit local reads feature cannot ' +
            'be used because libhadoop cannot be loaded.',
            'INFO Client: Requesting a new application from cluster with 10 ' +
            'NodeManagerapplication_1486558679801_1820s',
            'INFO Client: Submitting application application_1486558679801_1820 ' +
            'to ResourceManager'
        ]
        hook = SparkSubmitHook(conn_id='spark_yarn_cluster')
        hook._process_spark_submit_log(log_lines)
        hook.submit()

        # When
        hook.on_kill()

        # Then
        self.assertIn(call(['yarn', 'application', '-kill',
                            'application_1486558679801_1820'],
                           stderr=-1, stdout=-1),
                      mock_popen.mock_calls)

    def test_standalone_cluster_process_on_kill(self):
        # Given
        log_lines = [
            'Running Spark using the REST application submission protocol.',
            '17/11/28 11:14:15 INFO RestSubmissionClient: Submitting a request ' +
            'to launch an application in spark://spark-standalone-master:6066',
            '17/11/28 11:14:15 INFO RestSubmissionClient: Submission successfully ' +
            'created as driver-20171128111415-0001. Polling submission state...'
        ]
        hook = SparkSubmitHook(conn_id='spark_standalone_cluster')
        hook._process_spark_submit_log(log_lines)

        # When
        kill_cmd = hook._build_spark_driver_kill_command()

        # Then
        self.assertEqual(kill_cmd[0], '/path/to/spark_home/bin/spark-submit')
        self.assertEqual(kill_cmd[1], '--master')
        self.assertEqual(kill_cmd[2], 'spark://spark-standalone-master:6066')
        self.assertEqual(kill_cmd[3], '--kill')
        self.assertEqual(kill_cmd[4], 'driver-20171128111415-0001')

    @patch('airflow.contrib.kubernetes.kube_client.get_kube_client')
    @patch('airflow.contrib.hooks.spark_submit_hook.subprocess.Popen')
    def test_k8s_process_on_kill(self, mock_popen, mock_client_method):
        # Given
        mock_popen.return_value.stdout = six.StringIO('stdout')
        mock_popen.return_value.stderr = six.StringIO('stderr')
        mock_popen.return_value.poll.return_value = None
        mock_popen.return_value.wait.return_value = 0
        client = mock_client_method.return_value
        hook = SparkSubmitHook(conn_id='spark_k8s_cluster')
        log_lines = [
            'INFO  LoggingPodStatusWatcherImpl:54 - State changed, new state:' +
            'pod name: spark-pi-edf2ace37be7353a958b38733a12f8e6-driver' +
            'namespace: default' +
            'labels: spark-app-selector -> spark-465b868ada474bda82ccb84ab2747fcd,' +
            'spark-role -> driver' +
            'pod uid: ba9c61f6-205f-11e8-b65f-d48564c88e42' +
            'creation time: 2018-03-05T10:26:55Z' +
            'service account name: spark' +
            'volumes: spark-init-properties, download-jars-volume,' +
            'download-files-volume, spark-token-2vmlm' +
            'node name: N/A' +
            'start time: N/A' +
            'container images: N/A' +
            'phase: Pending' +
            'status: []' +
            '2018-03-05 11:26:56 INFO  LoggingPodStatusWatcherImpl:54 - State changed,' +
            ' new state:' +
            'pod name: spark-pi-edf2ace37be7353a958b38733a12f8e6-driver' +
            'namespace: default' +
            'Exit code: 0'
        ]
        hook._process_spark_submit_log(log_lines)
        hook.submit()

        # When
        hook.on_kill()

        # Then
        import kubernetes
        kwargs = {'pretty': True, 'body': kubernetes.client.V1DeleteOptions()}
        client.delete_namespaced_pod.assert_called_once_with(
            'spark-pi-edf2ace37be7353a958b38733a12f8e6-driver',
            'mynamespace', **kwargs)


if __name__ == '__main__':
    unittest.main()
