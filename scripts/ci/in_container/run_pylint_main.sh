#!/usr/bin/env bash
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
# shellcheck source=scripts/ci/in_container/_in_container_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/_in_container_script_init.sh"

export PYTHONPATH=${AIRFLOW_SOURCES}

set +e

if [[ ${#@} == "0" ]]; then
    echo
    echo "Running pylint for all sources except 'tests' folder"
    echo

    # Using path -prune is much better in the local environment on OSX because we have host
    # Files mounted and node_modules is a huge directory which takes many seconds to even scan
    # -prune works better than -not path because it skips traversing the whole directory. -not path traverses
    # the directory and only excludes it after all of it is scanned
    find . \
    -path "./airflow/www/node_modules" -prune -o \
    -path "./airflow/www_rbac/node_modules" -prune -o \
    -path "./airflow/migrations/versions" -prune -o \
    -path "./.eggs" -prune -o \
    -path "./docs/_build" -prune -o \
    -path "./build" -prune -o \
    -path "./tests" -prune -o \
    -name "*.py" \
    -not -name 'webserver_config.py' | \
        grep  ".*.py$" | \
        grep -vFf scripts/ci/pylint_todo.txt | xargs pylint --output-format=colorized
    RES=$?
else
    /usr/local/bin/pylint --output-format=colorized "$@"
    RES=$?
fi
set -e

if [[ "${RES}" != 0 ]]; then
    echo >&2
    echo >&2 "There were some pylint errors. Exiting"
    echo >&2
    exit 1
else
    echo
    echo "Pylint check succeeded"
    echo
fi
