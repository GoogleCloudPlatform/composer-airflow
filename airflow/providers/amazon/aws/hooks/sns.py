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

"""
This module contains AWS SNS hook
"""
import json
from typing import Optional, Union, Dict

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


def _get_message_attribute(o):
    if isinstance(o, bytes):
        return {'DataType': 'Binary', 'BinaryValue': o}
    if isinstance(o, str):
        return {'DataType': 'String', 'StringValue': o}
    if isinstance(o, (int, float)):
        return {'DataType': 'Number', 'StringValue': str(o)}
    if hasattr(o, '__iter__'):
        return {'DataType': 'String.Array', 'StringValue': json.dumps(o)}
    raise TypeError(
        'Values in MessageAttributes must be one of bytes, str, int, float, or iterable; ' f'got {type(o)}'
    )


class AwsSnsHook(AwsBaseHook):
    """
    Interact with Amazon Simple Notification Service.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs):
        super().__init__(client_type='sns', *args, **kwargs)

    def publish_to_target(
        self,
        target_arn: str,
        message: str,
        subject: Optional[str] = None,
        message_attributes: Optional[dict] = None,
    ):
        """
        Publish a message to a topic or an endpoint.

        :param target_arn: either a TopicArn or an EndpointArn
        :type target_arn: str
        :param message: the default message you want to send
        :param message: str
        :param subject: subject of message
        :type subject: str
        :param message_attributes: additional attributes to publish for message filtering. This should be
            a flat dict; the DataType to be sent depends on the type of the value:

            - bytes = Binary
            - str = String
            - int, float = Number
            - iterable = String.Array

        :type message_attributes: dict
        """
        publish_kwargs: Dict[str, Union[str, dict]] = {
            'TargetArn': target_arn,
            'MessageStructure': 'json',
            'Message': json.dumps({'default': message}),
        }

        # Construct args this way because boto3 distinguishes from missing args and those set to None
        if subject:
            publish_kwargs['Subject'] = subject
        if message_attributes:
            publish_kwargs['MessageAttributes'] = {
                key: _get_message_attribute(val) for key, val in message_attributes.items()
            }

        return self.get_conn().publish(**publish_kwargs)
