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
import re
import sys
from datetime import datetime
from typing import Dict, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.typing_compat import Protocol, runtime_checkable
from airflow.utils.decorators import apply_defaults


@runtime_checkable
class ECSProtocol(Protocol):
    """
    A structured Protocol for ``boto3.client('ecs')``. This is used for type hints on
    :py:meth:`.ECSOperator.client`.

    .. seealso::

        - https://mypy.readthedocs.io/en/latest/protocols.html
        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html
    """

    def run_task(self, **kwargs):
        """https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task"""  # noqa: E501  # pylint: disable=line-too-long
        ...

    def get_waiter(self, x: str):
        """https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.get_waiter"""  # noqa: E501  # pylint: disable=line-too-long
        ...

    def describe_tasks(self, cluster: str, tasks) -> Dict:
        """https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.describe_tasks"""  # noqa: E501  # pylint: disable=line-too-long
        ...

    def stop_task(self, cluster, task, reason: str) -> Dict:
        """https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.stop_task"""  # noqa: E501  # pylint: disable=line-too-long
        ...


class ECSOperator(BaseOperator):  # pylint: disable=too-many-instance-attributes
    """
    Execute a task on AWS ECS (Elastic Container Service)

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ECSOperator`

    :param task_definition: the task definition name on Elastic Container Service
    :type task_definition: str
    :param cluster: the cluster name on Elastic Container Service
    :type cluster: str
    :param overrides: the same parameter that boto3 will receive (templated):
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task
    :type overrides: dict
    :param aws_conn_id: connection id of AWS credentials / region name. If None,
        credential boto3 strategy will be used
        (http://boto3.readthedocs.io/en/latest/guide/configuration.html).
    :type aws_conn_id: str
    :param region_name: region name to use in AWS Hook.
        Override the region_name in connection (if provided)
    :type region_name: str
    :param launch_type: the launch type on which to run your task ('EC2' or 'FARGATE')
    :type launch_type: str
    :param group: the name of the task group associated with the task
    :type group: str
    :param placement_constraints: an array of placement constraint objects to use for
        the task
    :type placement_constraints: list
    :param platform_version: the platform version on which your task is running
    :type platform_version: str
    :param network_configuration: the network configuration for the task
    :type network_configuration: dict
    :param tags: a dictionary of tags in the form of {'tagKey': 'tagValue'}.
    :type tags: dict
    :param awslogs_group: the CloudWatch group where your ECS container logs are stored.
        Only required if you want logs to be shown in the Airflow UI after your job has
        finished.
    :type awslogs_group: str
    :param awslogs_region: the region in which your CloudWatch logs are stored.
        If None, this is the same as the `region_name` parameter. If that is also None,
        this is the default AWS region based on your connection settings.
    :type awslogs_region: str
    :param awslogs_stream_prefix: the stream prefix that is used for the CloudWatch logs.
        This is usually based on some custom name combined with the name of the container.
        Only required if you want logs to be shown in the Airflow UI after your job has
        finished.
    :type awslogs_stream_prefix: str
    """

    ui_color = '#f0ede4'
    client = None  # type: Optional[ECSProtocol]
    arn = None  # type: Optional[str]
    template_fields = ('overrides',)

    @apply_defaults
    def __init__(self, *, task_definition, cluster, overrides,  # pylint: disable=too-many-arguments
                 aws_conn_id=None, region_name=None, launch_type='EC2',
                 group=None, placement_constraints=None, platform_version='LATEST',
                 network_configuration=None, tags=None, awslogs_group=None,
                 awslogs_region=None, awslogs_stream_prefix=None, propagate_tags=None, **kwargs):
        super().__init__(**kwargs)

        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.task_definition = task_definition
        self.cluster = cluster
        self.overrides = overrides
        self.launch_type = launch_type
        self.group = group
        self.placement_constraints = placement_constraints
        self.platform_version = platform_version
        self.network_configuration = network_configuration

        self.tags = tags
        self.awslogs_group = awslogs_group
        self.awslogs_stream_prefix = awslogs_stream_prefix
        self.awslogs_region = awslogs_region
        self.propagate_tags = propagate_tags

        if self.awslogs_region is None:
            self.awslogs_region = region_name

        self.hook = None

    def execute(self, context):
        self.log.info(
            'Running ECS Task - Task definition: %s - on cluster %s',
            self.task_definition, self.cluster
        )
        self.log.info('ECSOperator overrides: %s', self.overrides)

        self.client = self.get_hook().get_conn()

        run_opts = {
            'cluster': self.cluster,
            'taskDefinition': self.task_definition,
            'overrides': self.overrides,
            'startedBy': self.owner,
        }

        if self.launch_type:
            run_opts['launchType'] = self.launch_type
            if self.launch_type == 'FARGATE':
                run_opts['platformVersion'] = self.platform_version
        if self.group is not None:
            run_opts['group'] = self.group
        if self.placement_constraints is not None:
            run_opts['placementConstraints'] = self.placement_constraints
        if self.network_configuration is not None:
            run_opts['networkConfiguration'] = self.network_configuration
        if self.tags is not None:
            run_opts['tags'] = [{'key': k, 'value': v} for (k, v) in self.tags.items()]
        if self.propagate_tags is not None:
            run_opts['propagateTags'] = self.propagate_tags

        response = self.client.run_task(**run_opts)

        failures = response['failures']
        if len(failures) > 0:
            raise AirflowException(response)
        self.log.info('ECS Task started: %s', response)

        self.arn = response['tasks'][0]['taskArn']
        self._wait_for_task_ended()

        self._check_success_task()
        self.log.info('ECS Task has been successfully executed: %s', response)

    def _wait_for_task_ended(self):
        waiter = self.client.get_waiter('tasks_stopped')
        waiter.config.max_attempts = sys.maxsize  # timeout is managed by airflow
        waiter.wait(
            cluster=self.cluster,
            tasks=[self.arn]
        )

    def _check_success_task(self):
        response = self.client.describe_tasks(
            cluster=self.cluster,
            tasks=[self.arn]
        )
        self.log.info('ECS Task stopped, check status: %s', response)

        # Get logs from CloudWatch if the awslogs log driver was used
        if self.awslogs_group and self.awslogs_stream_prefix:
            self.log.info('ECS Task logs output:')
            task_id = self.arn.split("/")[-1]
            stream_name = "{}/{}".format(self.awslogs_stream_prefix, task_id)
            for event in self.get_logs_hook().get_log_events(self.awslogs_group, stream_name):
                event_dt = datetime.fromtimestamp(event['timestamp'] / 1000.0)
                self.log.info("[%s] %s", event_dt.isoformat(), event['message'])

        if len(response.get('failures', [])) > 0:
            raise AirflowException(response)

        for task in response['tasks']:
            # This is a `stoppedReason` that indicates a task has not
            # successfully finished, but there is no other indication of failure
            # in the response.
            # https://docs.aws.amazon.com/AmazonECS/latest/developerguide/stopped-task-errors.html
            if re.match(r'Host EC2 \(instance .+?\) (stopped|terminated)\.',
                        task.get('stoppedReason', '')):
                raise AirflowException(
                    'The task was stopped because the host instance terminated: {}'.
                    format(task.get('stoppedReason', '')))
            containers = task['containers']
            for container in containers:
                if container.get('lastStatus') == 'STOPPED' and \
                        container['exitCode'] != 0:
                    raise AirflowException(
                        'This task is not in success state {}'.format(task))
                elif container.get('lastStatus') == 'PENDING':
                    raise AirflowException('This task is still pending {}'.format(task))
                elif 'error' in container.get('reason', '').lower():
                    raise AirflowException(
                        'This containers encounter an error during launching : {}'.
                        format(container.get('reason', '').lower()))

    def get_hook(self):
        """Create and return an AwsHook."""
        if not self.hook:
            self.hook = AwsBaseHook(
                aws_conn_id=self.aws_conn_id,
                client_type='ecs',
                region_name=self.region_name
            )
        return self.hook

    def get_logs_hook(self):
        """Create and return an AwsLogsHook."""
        return AwsLogsHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.awslogs_region
        )

    def on_kill(self):
        response = self.client.stop_task(
            cluster=self.cluster,
            task=self.arn,
            reason='Task killed by the user')
        self.log.info(response)
