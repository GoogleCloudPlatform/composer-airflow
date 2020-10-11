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
if [[ ${VERBOSE_COMMANDS:="false"} == "true" ]]; then
    set -x
fi

function disable_rbac_if_requested() {
    if [[ ${DISABLE_RBAC:="false"} == "true" ]]; then
        export AIRFLOW__WEBSERVER__RBAC="False"
    else
        export AIRFLOW__WEBSERVER__RBAC="True"
    fi
}


# shellcheck source=scripts/in_container/_in_container_script_init.sh
. /opt/airflow/scripts/in_container/_in_container_script_init.sh

AIRFLOW_SOURCES=$(cd "${IN_CONTAINER_DIR}/../.." || exit 1; pwd)

PYTHON_MAJOR_MINOR_VERSION=${PYTHON_MAJOR_MINOR_VERSION:=3.6}
BACKEND=${BACKEND:=sqlite}

export AIRFLOW_HOME=${AIRFLOW_HOME:=${HOME}}

: "${AIRFLOW_SOURCES:?"ERROR: AIRFLOW_SOURCES not set !!!!"}"

echo
echo "Airflow home: ${AIRFLOW_HOME}"
echo "Airflow sources: ${AIRFLOW_SOURCES}"
echo "Airflow core SQL connection: ${AIRFLOW__CORE__SQL_ALCHEMY_CONN:=}"
if [[ -n "${AIRFLOW__CORE__SQL_ENGINE_COLLATION_FOR_IDS=}" ]]; then
    echo "Airflow collation for IDs: ${AIRFLOW__CORE__SQL_ENGINE_COLLATION_FOR_IDS}"
fi

echo

RUN_TESTS=${RUN_TESTS:="false"}
CI=${CI:="false"}
INSTALL_AIRFLOW_VERSION="${INSTALL_AIRFLOW_VERSION:=""}"

if [[ ${GITHUB_ACTIONS:="false"} == "false" ]]; then
    # Create links for useful CLI tools
    # shellcheck source=scripts/in_container/run_cli_tool.sh
    source <(bash scripts/in_container/run_cli_tool.sh)
fi

if [[ ${AIRFLOW_VERSION} == *1.10* || ${INSTALL_AIRFLOW_VERSION} == *1.10* ]]; then
    export RUN_AIRFLOW_1_10="true"
else
    export RUN_AIRFLOW_1_10="false"
fi

if [[ -z ${INSTALL_AIRFLOW_VERSION=} ]]; then
    if [[ ! -d "${AIRFLOW_SOURCES}/airflow/www/node_modules" ]]; then
        echo
        echo "Installing node modules as they are not yet installed (Sources mounted from Host)"
        echo
        pushd "${AIRFLOW_SOURCES}/airflow/www/" &>/dev/null || exit 1
        yarn install --frozen-lockfile
        echo
        popd &>/dev/null || exit 1
    fi
    if [[ ! -d "${AIRFLOW_SOURCES}/airflow/www/static/dist" ]]; then
        pushd "${AIRFLOW_SOURCES}/airflow/www/" &>/dev/null || exit 1
        echo
        echo "Building production version of JavaScript files (Sources mounted from Host)"
        echo
        echo
        yarn run prod
        echo
        echo
        popd &>/dev/null || exit 1
    fi
    # Cleanup the logs, tmp when entering the environment
    sudo rm -rf "${AIRFLOW_SOURCES}"/logs/*
    sudo rm -rf "${AIRFLOW_SOURCES}"/tmp/*
    mkdir -p "${AIRFLOW_SOURCES}"/logs/
    mkdir -p "${AIRFLOW_SOURCES}"/tmp/
    export PYTHONPATH=${AIRFLOW_SOURCES}
else
    install_released_airflow_version "${INSTALL_AIRFLOW_VERSION}"
fi

if [[ ${INSTALL_WHEELS} == "true" ]]; then
  pip install /dist/*.whl || true
fi

export RUN_AIRFLOW_1_10=${RUN_AIRFLOW_1_10:="false"}

# Added to have run-tests on path
export PATH=${PATH}:${AIRFLOW_SOURCES}

# This is now set in conftest.py - only for pytest tests
unset AIRFLOW__CORE__UNIT_TEST_MODE

mkdir -pv "${AIRFLOW_HOME}/logs/"
cp -f "${IN_CONTAINER_DIR}/airflow_ci.cfg" "${AIRFLOW_HOME}/unittests.cfg"

disable_rbac_if_requested

set +e
"${IN_CONTAINER_DIR}/check_environment.sh"
ENVIRONMENT_EXIT_CODE=$?
set -e
if [[ ${ENVIRONMENT_EXIT_CODE} != 0 ]]; then
    echo
    echo "Error: check_environment returned ${ENVIRONMENT_EXIT_CODE}. Exiting."
    echo
    exit ${ENVIRONMENT_EXIT_CODE}
fi


if [[ ${INTEGRATION_KERBEROS:="false"} == "true" ]]; then
    set +e
    setup_kerberos
    RES=$?
    set -e

    if [[ ${RES} != 0 ]]; then
        echo
        echo "ERROR !!!!Kerberos initialisation requested, but failed"
        echo
        echo "I will exit now, and you need to run 'breeze --integration kerberos restart'"
        echo "to re-enter breeze and restart kerberos."
        echo
        exit 1
    fi
fi

# Create symbolic link to fix possible issues with kubectl config cmd-path
mkdir -p /usr/lib/google-cloud-sdk/bin
touch /usr/lib/google-cloud-sdk/bin/gcloud
ln -s -f /usr/bin/gcloud /usr/lib/google-cloud-sdk/bin/gcloud

# Set up ssh keys
echo 'yes' | ssh-keygen -t rsa -C your_email@youremail.com -m PEM -P '' -f ~/.ssh/id_rsa \
    >"${AIRFLOW_HOME}/logs/ssh-keygen.log" 2>&1

cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
ln -s -f ~/.ssh/authorized_keys ~/.ssh/authorized_keys2
chmod 600 ~/.ssh/*

# SSH Service
sudo service ssh restart >/dev/null 2>&1

# Sometimes the server is not quick enough to load the keys!
while [[ $(ssh-keyscan -H localhost 2>/dev/null | wc -l) != "3" ]] ; do
    echo "Not all keys yet loaded by the server"
    sleep 0.05
done

ssh-keyscan -H localhost >> ~/.ssh/known_hosts 2>/dev/null

# shellcheck source=scripts/in_container/configure_environment.sh
. "${IN_CONTAINER_DIR}/configure_environment.sh"

# shellcheck source=scripts/in_container/run_init_script.sh
. "${IN_CONTAINER_DIR}/run_init_script.sh"

# shellcheck source=scripts/in_container/run_tmux.sh
. "${IN_CONTAINER_DIR}/run_tmux.sh"

cd "${AIRFLOW_SOURCES}"

set +u
# If we do not want to run tests, we simply drop into bash
if [[ "${RUN_TESTS}" != "true" ]]; then
    exec /bin/bash "${@}"
fi
set -u

export RESULT_LOG_FILE="/files/test_result.xml"

if [[ "${GITHUB_ACTIONS}" == "true" ]]; then
    EXTRA_PYTEST_ARGS=(
        "--verbosity=0"
        "--strict-markers"
        "--durations=100"
        "--cov=airflow/"
        "--cov-config=.coveragerc"
        "--cov-report=xml:/files/coverage.xml"
        "--color=yes"
        "--maxfail=50"
        "--pythonwarnings=ignore::DeprecationWarning"
        "--pythonwarnings=ignore::PendingDeprecationWarning"
        "--junitxml=${RESULT_LOG_FILE}"
        # timeouts in seconds for individual tests
        "--setup-timeout=20"
        "--execution-timeout=60"
        "--teardown-timeout=20"
        # Only display summary for non-expected case
        # f - failed
        # E - error
        # X - xpessed (passed even if expected to fail)
        # The following cases are not displayed:
        # s - skipped
        # x - xfailed (expected to fail and failed)
        # p - passed
        # P - passed with output
        "-rfEX"
        )
else
    EXTRA_PYTEST_ARGS=(
        "-rfEX"
    )
fi

declare -a SELECTED_TESTS CLI_TESTS API_TESTS PROVIDERS_TESTS CORE_TESTS WWW_TESTS \
    ALL_TESTS ALL_PRESELECTED_TESTS ALL_OTHER_TESTS

# Finds all directories that are not on the list of tests
# - so that we do not skip any in the future if new directories are added
function find_all_other_tests() {
    local all_tests_dirs
    all_tests_dirs=$(find "tests" -type d)
    all_tests_dirs=$(echo "${all_tests_dirs}" | sed "/tests$/d" )
    all_tests_dirs=$(echo "${all_tests_dirs}" | sed "/tests\/dags/d" )
    local path
    for path in "${ALL_PRESELECTED_TESTS[@]}"
    do
        escaped_path="${path//\//\\\/}"
        all_tests_dirs=$(echo "${all_tests_dirs}" | sed "/${escaped_path}/d" )
    done
    for path in ${all_tests_dirs}
    do
        ALL_OTHER_TESTS+=("${path}")
    done
}

if [[ ${#@} -gt 0 && -n "$1" ]]; then
    SELECTED_TESTS=("${@}")
else
    CLI_TESTS=("tests/cli")
    API_TESTS=("tests/api" "tests/api_connexion")
    PROVIDERS_TESTS=("tests/providers")
    CORE_TESTS=(
        "tests/core"
        "tests/executors"
        "tests/jobs"
        "tests/models"
        "tests/serialization"
        "tests/ti_deps"
        "tests/utils"
    )
    WWW_TESTS=("tests/www")
    ALL_TESTS=("tests")
    ALL_PRESELECTED_TESTS=(
        "${CLI_TESTS[@]}"
        "${API_TESTS[@]}"
        "${PROVIDERS_TESTS[@]}"
        "${CORE_TESTS[@]}"
        "${WWW_TESTS[@]}"
    )

    if [[ ${TEST_TYPE:=""} == "CLI" ]]; then
        SELECTED_TESTS=("${CLI_TESTS[@]}")
    elif [[ ${TEST_TYPE:=""} == "API" ]]; then
        SELECTED_TESTS=("${API_TESTS[@]}")
    elif [[ ${TEST_TYPE:=""} == "Providers" ]]; then
        SELECTED_TESTS=("${PROVIDERS_TESTS[@]}")
    elif [[ ${TEST_TYPE:=""} == "Core" ]]; then
        SELECTED_TESTS=("${CORE_TESTS[@]}")
    elif [[ ${TEST_TYPE:=""} == "WWW" ]]; then
        SELECTED_TESTS=("${WWW_TESTS[@]}")
    elif [[ ${TEST_TYPE:=""} == "Other" ]]; then
        find_all_other_tests
        SELECTED_TESTS=("${ALL_OTHER_TESTS[@]}")
    elif [[ ${TEST_TYPE:=""} == "All" || ${TEST_TYPE} == "Quarantined" || \
            ${TEST_TYPE} == "Postgres" || ${TEST_TYPE} == "MySQL" || \
            ${TEST_TYPE} == "Heisentests" || ${TEST_TYPE} == "Long" || \
            ${TEST_TYPE} == "Integration" ]]; then
        SELECTED_TESTS=("${ALL_TESTS[@]}")
    else
        >&2 echo
        >&2 echo "Wrong test type ${TEST_TYPE}"
        >&2 echo
        exit 1
    fi

fi
readonly SELECTED_TESTS CLI_TESTS API_TESTS PROVIDERS_TESTS CORE_TESTS WWW_TESTS \
    ALL_TESTS ALL_PRESELECTED_TESTS

if [[ -n ${RUN_INTEGRATION_TESTS=} ]]; then
    # Integration tests
    for INT in ${RUN_INTEGRATION_TESTS}
    do
        EXTRA_PYTEST_ARGS+=("--integration" "${INT}")
    done
elif [[ ${TEST_TYPE:=""} == "Long" ]]; then
    EXTRA_PYTEST_ARGS+=(
        "-m" "long_running"
        "--include-long-running"
    )
elif [[ ${TEST_TYPE:=""} == "Heisentests" ]]; then
    EXTRA_PYTEST_ARGS+=(
        "-m" "heisentests"
        "--include-heisentests"
    )
elif [[ ${TEST_TYPE:=""} == "Postgres" ]]; then
    EXTRA_PYTEST_ARGS+=(
        "--backend"
        "postgres"
    )
elif [[ ${TEST_TYPE:=""} == "MySQL" ]]; then
    EXTRA_PYTEST_ARGS+=(
        "--backend"
        "mysql"
    )
elif [[ ${TEST_TYPE:=""} == "Quarantined" ]]; then
    EXTRA_PYTEST_ARGS+=(
        "-m" "quarantined"
        "--include-quarantined"
    )
fi

echo
echo "Running tests ${SELECTED_TESTS[*]}"
echo

ARGS=("${EXTRA_PYTEST_ARGS[@]}" "${SELECTED_TESTS[@]}")

if [[ ${RUN_SYSTEM_TESTS:="false"} == "true" ]]; then
    "${IN_CONTAINER_DIR}/run_system_tests.sh" "${ARGS[@]}"
else
    "${IN_CONTAINER_DIR}/run_ci_tests.sh" "${ARGS[@]}"
fi
