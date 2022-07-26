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
# Skip printing groups in CI
PRINT_INFO_FROM_SCRIPTS="false"
# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

PRINT_INFO_FROM_SCRIPTS="true"
export PRINT_INFO_FROM_SCRIPTS

DOCKER_COMPOSE_LOCAL=()
INTEGRATIONS=()
INTEGRATION_BREEZE_FLAGS=()

function prepare_tests() {
    DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/files.yml")
    if [[ ${MOUNT_SELECTED_LOCAL_SOURCES} == "true" ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/local.yml")
    fi
    if [[ ${MOUNT_ALL_LOCAL_SOURCES} == "true" ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/local-all-sources.yml")
    fi

    if [[ ${GITHUB_ACTIONS=} == "true" ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/ga.yml")
    fi

    if [[ ${FORWARD_CREDENTIALS} == "true" ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/forward-credentials.yml")
    fi

    if [[ -n ${USE_AIRFLOW_VERSION=} ]]; then
        DOCKER_COMPOSE_LOCAL+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/remove-sources.yml")
    fi

    readonly DOCKER_COMPOSE_LOCAL

    if [[ ${TEST_TYPE:=} == "Integration" ]]; then
        export ENABLED_INTEGRATIONS="${AVAILABLE_INTEGRATIONS}"
        export LIST_OF_INTEGRATION_TESTS_TO_RUN="${AVAILABLE_INTEGRATIONS}"
    else
        export ENABLED_INTEGRATIONS=""
        export LIST_OF_INTEGRATION_TESTS_TO_RUN=""
    fi

    for _INT in ${ENABLED_INTEGRATIONS}
    do
        INTEGRATIONS+=("-f" "${SCRIPTS_CI_DIR}/docker-compose/integration-${_INT}.yml")
        INTEGRATION_BREEZE_FLAGS+=("--integration" "${_INT}")
    done

    readonly INTEGRATIONS

    echo "**********************************************************************************************"
    echo
    echo "      TEST_TYPE: ${TEST_TYPE}, ENABLED INTEGRATIONS: ${ENABLED_INTEGRATIONS}"
    echo
    echo "**********************************************************************************************"
}


# Runs airflow testing in docker container
# You need to set variable TEST_TYPE - test type to run
# "${@}" - extra arguments to pass to docker command
function run_airflow_testing_in_docker() {
    set +u
    set +e
    local exit_code
    echo
    echo "Semaphore grabbed. Running tests for ${TEST_TYPE}"
    echo

    echo "Making sure docker-compose is down and remnants removed"
    echo
    docker-compose -f "${SCRIPTS_CI_DIR}/docker-compose/base.yml" \
        "${INTEGRATIONS[@]}" \
        --project-name "airflow-${TEST_TYPE/\[*\]/}-${BACKEND}" \
        down --remove-orphans \
        --volumes --timeout 10
    docker-compose --log-level INFO \
      -f "${SCRIPTS_CI_DIR}/docker-compose/base.yml" \
      "${BACKEND_DOCKER_COMPOSE[@]}" \
      "${INTEGRATIONS[@]}" \
      "${DOCKER_COMPOSE_LOCAL[@]}" \
      --project-name "airflow-${TEST_TYPE/\[*\]/}-${BACKEND}" \
         run airflow "${@}"
    exit_code=$?
    docker ps
    if [[ ${exit_code} != "0" && ${CI} == "true" ]]; then
        docker ps --all
        local container
        for container in $(docker ps --all --format '{{.Names}}')
        do
            testing::dump_container_logs "${container}"
        done
    fi

    docker-compose --log-level INFO -f "${SCRIPTS_CI_DIR}/docker-compose/base.yml" \
        "${INTEGRATIONS[@]}" \
        --project-name "airflow-${TEST_TYPE/\[*\]/}-${BACKEND}" \
        down --remove-orphans \
        --volumes --timeout 10
    set -u
    set -e
    if [[ ${exit_code} != "0" ]]; then
        EXTRA_ARGS=""
        if [[ ${BACKEND} == "postgres" ]]; then
            EXTRA_ARGS="--postgres-version ${POSTGRES_VERSION} "
        elif [[ ${BACKEND} == "mysql" ]]; then
            EXTRA_ARGS="--mysql-version ${MYSQL_VERSION} "
        fi
        echo "${COLOR_RED}***********************************************************************************************${COLOR_RESET}"
        echo "${COLOR_RED}*${COLOR_RESET}"
        echo "${COLOR_RED}* ERROR! Some tests failed, unfortunately. Those might be transient errors,${COLOR_RESET}"
        echo "${COLOR_RED}*        but usually you have to fix something.${COLOR_RESET}"
        echo "${COLOR_RED}*        See the above log for details.${COLOR_RESET}"
        echo "${COLOR_RED}*${COLOR_RESET}"
        echo "${COLOR_RED}***********************************************************************************************${COLOR_RESET}"
        echo
        echo "${COLOR_BLUE}***********************************************************************************************${COLOR_RESET}"
        echo "${COLOR_BLUE}Enter the same environment that was used for the tests:${COLOR_RESET}"
        echo "${COLOR_YELLOW}breeze --image-tag ${IMAGE_TAG=} --backend ${BACKEND} ${EXTRA_ARGS}--python ${PYTHON_MAJOR_MINOR_VERSION} --db-reset --skip-mounting-local-sources --test-type ${TEST_TYPE} ${INTEGRATION_BREEZE_FLAGS[*]} shell${COLOR_RESET}"
        echo "${COLOR_BLUE}Then you can run failed tests with:${COLOR_RESET}"
        echo "${COLOR_YELLOW}pytest [TEST_NAME]${COLOR_RESET}"
        echo "${COLOR_BLUE}Or you can run the tests: ${COLOR_RESET}"
        echo "${COLOR_YELLOW}breeze --image-tag ${IMAGE_TAG=} --backend ${BACKEND} ${EXTRA_ARGS}--python ${PYTHON_MAJOR_MINOR_VERSION} --db-reset --skip-mounting-local-sources --test-type ${TEST_TYPE} ${INTEGRATION_BREEZE_FLAGS[*]} test${COLOR_RESET}"
        echo "${COLOR_BLUE}***********************************************************************************************${COLOR_RESET}"


        if [[ ${UPGRADE_TO_NEWER_DEPENDENCIES} != "false" ]]; then
            local constraints_url="https://raw.githubusercontent.com/apache/airflow/${DEFAULT_CONSTRAINTS_BRANCH}/constraints-source-providers-${PYTHON_MAJOR_MINOR_VERSION}.txt"
            echo "${COLOR_BLUE}***********************************************************************************************${COLOR_RESET}"
            echo "${COLOR_BLUE}*${COLOR_RESET}"
            echo "${COLOR_BLUE}* In case you see unrelated test failures, it can be due to newer dependencies released.${COLOR_RESET}"
            echo "${COLOR_BLUE}* This is either because it is 'main' branch or because this PR modifies dependencies (setup.* files).${COLOR_RESET}"
            echo "${COLOR_BLUE}* Therefore 'eager-upgrade' is used to build the image, This means that this build can have newer dependencies than the 'tested' set of constraints,${COLOR_RESET}"
            echo "${COLOR_BLUE}*${COLOR_RESET}"
            echo "${COLOR_BLUE}* The tested constraints for that build are available at: ${constraints_url} ${COLOR_RESET}"
            echo "${COLOR_BLUE}*${COLOR_RESET}"
            echo "${COLOR_BLUE}* Please double check if the same failure is in other tests and in 'main' branch and check if the dependency differences causes the problem.${COLOR_RESET}"
            echo "${COLOR_BLUE}* In case you identify the dependency, either fix the root cause or limit the dependency if it is too difficult to fix.${COLOR_RESET}"
            echo "${COLOR_BLUE}*${COLOR_RESET}"
            echo "${COLOR_BLUE}* The diff between fixed constraints and those used in this build is below.${COLOR_RESET}"
            echo "${COLOR_BLUE}*${COLOR_RESET}"
            echo "${COLOR_BLUE}***********************************************************************************************${COLOR_RESET}"
            echo
            curl "${constraints_url}" | grep -ve "^#" | diff --color=always - <( docker run --entrypoint /bin/bash "${AIRFLOW_CI_IMAGE_WITH_TAG}"  -c 'pip freeze' \
                | sort | grep -v "apache_airflow" | grep -v "@" | grep -v "/opt/airflow" | grep -ve "^#")
            echo
        fi
    fi

    echo ${exit_code} > "${PARALLEL_JOB_STATUS}"

    if [[ ${exit_code} == 0 ]]; then
        echo
        echo "${COLOR_GREEN}Test type: ${TEST_TYPE} succeeded.${COLOR_RESET}"
    else
        echo
        echo "${COLOR_RED}Test type: ${TEST_TYPE} failed.${COLOR_RESET}"
    fi
    return "${exit_code}"
}

prepare_tests

testing::setup_docker_compose_backend "${TEST_TYPE}"
run_airflow_testing_in_docker "${@}"
