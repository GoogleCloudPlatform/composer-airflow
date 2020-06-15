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
from datetime import datetime, timedelta
import json
import logging
import os
import time


def __load__environment_variables__():
    with open('/home/airflow/gcs/env_var.json', 'r') as env_var_json:
        os.environ.update(json.load(env_var_json))
    logging.info('Composer Environment Variables have been loaded.')


def init_env_vars():
    # Wait up to 25 seconds for env_var.json to sync
    # (gsutil cp to the local filesystem)
    timeout_at = datetime.now() + timedelta(seconds=25)

    while datetime.now() < timeout_at:
        try:
            __load__environment_variables__()
            break
        except:
            time.sleep(1)
    else:
        try:
            __load__environment_variables__()
        except:
            logging.warning('Can\'t load Environment Variable overrides.',
                            exc_info=True)
            logging.warning('Using default Composer Environment Variables. Overrides '
                            'have not been applied.')
