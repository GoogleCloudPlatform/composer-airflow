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

"""This module contains AWS CloudFormation Hook"""
import warnings
from typing import Optional, Union

from boto3 import client, resource
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class CloudFormationHook(AwsBaseHook):
    """
    Interact with AWS CloudFormation.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs):
        super().__init__(client_type='cloudformation', *args, **kwargs)

    def get_stack_status(self, stack_name: Union[client, resource]) -> Optional[dict]:
        """Get stack status from CloudFormation."""
        self.log.info('Poking for stack %s', stack_name)

        try:
            stacks = self.get_conn().describe_stacks(StackName=stack_name)['Stacks']
            return stacks[0]['StackStatus']
        except ClientError as e:
            if 'does not exist' in str(e):
                return None
            else:
                raise e

    def create_stack(self, stack_name: str, cloudformation_parameters: dict) -> None:
        """
        Create stack in CloudFormation.

        :param stack_name: stack_name.
        :type stack_name: str
        :param cloudformation_parameters: parameters to be passed to CloudFormation.
        :type cloudformation_parameters: dict
        """
        if 'StackName' not in cloudformation_parameters:
            cloudformation_parameters['StackName'] = stack_name
        self.get_conn().create_stack(**cloudformation_parameters)

    def delete_stack(self, stack_name: str, cloudformation_parameters: Optional[dict] = None) -> None:
        """
        Delete stack in CloudFormation.

        :param stack_name: stack_name.
        :type stack_name: str
        :param cloudformation_parameters: parameters to be passed to CloudFormation (optional).
        :type cloudformation_parameters: dict
        """
        cloudformation_parameters = cloudformation_parameters or {}
        if 'StackName' not in cloudformation_parameters:
            cloudformation_parameters['StackName'] = stack_name
        self.get_conn().delete_stack(**cloudformation_parameters)


class AWSCloudFormationHook(CloudFormationHook):
    """
    This hook is deprecated.
    Please use :class:`airflow.providers.amazon.aws.hooks.cloud_formation.CloudFormationHook`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "This hook is deprecated. "
            "Please use :class:`airflow.providers.amazon.aws.hooks.cloud_formation.CloudFormationHook`.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)
