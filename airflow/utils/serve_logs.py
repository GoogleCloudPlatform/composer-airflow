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

"""Serve logs process"""
import os

import flask
from setproctitle import setproctitle

from airflow.configuration import conf


def serve_logs():
    """Serves logs generated by Worker"""
    print("Starting flask")
    flask_app = flask.Flask(__name__)
    setproctitle("airflow serve-logs")

    @flask_app.route('/log/<path:filename>')
    def serve_logs_view(filename):
        log_directory = os.path.expanduser(conf.get('logging', 'BASE_LOG_FOLDER'))
        return flask.send_from_directory(
            log_directory, filename, mimetype="application/json", as_attachment=False
        )

    worker_log_server_port = conf.getint('celery', 'WORKER_LOG_SERVER_PORT')
    flask_app.run(host='0.0.0.0', port=worker_log_server_port)
