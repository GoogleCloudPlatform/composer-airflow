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
from datetime import datetime
from os import environ

from airflow import DAG
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator

SNS_TOPIC_ARN = environ.get('SNS_TOPIC_ARN', 'arn:aws:sns:us-west-2:123456789012:dummy-topic-name')

with DAG(
    dag_id='example_sns',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:

    # [START howto_operator_sns_publish_operator]
    publish = SnsPublishOperator(
        task_id='publish_message',
        target_arn=SNS_TOPIC_ARN,
        message='This is a sample message sent to SNS via an Apache Airflow DAG task.',
    )
    # [END howto_operator_sns_publish_operator]
