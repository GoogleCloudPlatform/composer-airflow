# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import ssl

from airflow.exceptions import AirflowConfigException, AirflowException
from airflow import configuration
from airflow.utils.log.logging_mixin import LoggingMixin


DEFAULT_CELERY_CONFIG = {
    'accept_content': ['json', 'pickle'],
    'event_serializer': 'json',
    'result_serializer': 'pickle',
    'worker_prefetch_multiplier': 1,
    'task_acks_late': True,
    'task_default_queue': configuration.get('celery', 'DEFAULT_QUEUE'),
    'task_default_exchange': configuration.get('celery', 'DEFAULT_QUEUE'),
    'broker_url': configuration.get('celery', 'BROKER_URL'),
    'broker_transport_options': {'visibility_timeout': 21600},
    'result_backend': configuration.get('celery', 'CELERY_RESULT_BACKEND'),
    'worker_concurrency': configuration.getint('celery', 'CELERYD_CONCURRENCY'),
}

celery_ssl_active = False
try:
    celery_ssl_active = configuration.getboolean('celery', 'CELERY_SSL_ACTIVE')
except AirflowConfigException as e:
    log = LoggingMixin().log
    log.warning("Celery Executor will run without SSL")

try:
    if celery_ssl_active:
        broker_use_ssl = {'keyfile': configuration.get('celery', 'CELERY_SSL_KEY'),
                          'certfile': configuration.get('celery', 'CELERY_SSL_CERT'),
                          'ca_certs': configuration.get('celery', 'CELERY_SSL_CACERT'),
                          'cert_reqs': ssl.CERT_REQUIRED}
        DEFAULT_CELERY_CONFIG['broker_use_ssl'] = broker_use_ssl
except AirflowConfigException as e:
    raise AirflowException('AirflowConfigException: CELERY_SSL_ACTIVE is True, '
                           'please ensure CELERY_SSL_KEY, '
                           'CELERY_SSL_CERT and CELERY_SSL_CACERT are set')
except Exception as e:
    raise AirflowException('Exception: There was an unknown Celery SSL Error. '
                           'Please ensure you want to use '
                           'SSL and/or have all necessary certs and key ({}).'.format(e))

