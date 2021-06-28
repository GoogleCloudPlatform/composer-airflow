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

# shellcheck disable=SC2030,SC2031
: "${PYTHON_MAJOR_MINOR_VERSION:?"ERROR: PYTHON_MAJOR_MINOR_VERSION not set !!!!"}"
: "${INSTALL_AIRFLOW_VERSION:?"ERROR: INSTALL_AIRFLOW_VERSION not set !!!!"}"
export FORCE_ANSWER_TO_QUESTIONS="yes"
export VERBOSE="true"
# This is an image built from the "release" tag (either RC or final one).
# In this case all packages are taken from PyPI rather than from locally built sources
export INSTALL_FROM_PYPI="true"
export INSTALL_FROM_DOCKER_CONTEXT_FILES="false"
export INSTALL_PROVIDERS_FROM_SOURCES="false"
export AIRFLOW_PRE_CACHED_PIP_PACKAGES="false"
export DOCKER_CACHE="local"
export FORCE_PULL_BASE_PYTHON_IMAGE="true"
export DOCKER_TAG=${INSTALL_AIRFLOW_VERSION}-python${PYTHON_MAJOR_MINOR_VERSION}
# Name the image based on the TAG rather than based on the branch name
export FORCE_AIRFLOW_PROD_BASE_TAG="${DOCKER_TAG}"
export AIRFLOW_CONSTRAINTS_REFERENCE="constraints-${INSTALL_AIRFLOW_VERSION}"
export AIRFLOW_CONSTRAINTS="constraints"
# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"
echo
echo "Building and pushing ${INSTALL_AIRFLOW_VERSION} Airflow PROD image for ${PYTHON_MAJOR_MINOR_VERSION}"
echo
rm -rf "${BUILD_CACHE_DIR}"
rm -rf "${AIRFLOW_SOURCES}/docker-context-files/*"
build_images::prepare_prod_build
build_images::build_prod_images
verify_image::verify_prod_image "${AIRFLOW_PROD_IMAGE}"
echo
echo "Pushing airflow image as apache/airflow:${INSTALL_AIRFLOW_VERSION}-python${PYTHON_MAJOR_MINOR_VERSION}"
echo
# Re-tag the image to be published in "apache/airflow"
docker tag "apache/airflow-ci:${INSTALL_AIRFLOW_VERSION}-python${PYTHON_MAJOR_MINOR_VERSION}" \
     "apache/airflow:${INSTALL_AIRFLOW_VERSION}-python${PYTHON_MAJOR_MINOR_VERSION}"
docker push "apache/airflow:${INSTALL_AIRFLOW_VERSION}-python${PYTHON_MAJOR_MINOR_VERSION}"
if [[ ${PYTHON_MAJOR_MINOR_VERSION} == "${DEFAULT_PYTHON_MAJOR_MINOR_VERSION}" ]]; then
    echo
    echo "Pushing default airflow image as apache/airflow:${INSTALL_AIRFLOW_VERSION}"
    echo
    # In case of default Python version we also push ":version" tag
    docker tag "apache/airflow:${INSTALL_AIRFLOW_VERSION}-python${PYTHON_MAJOR_MINOR_VERSION}" \
        "apache/airflow:${INSTALL_AIRFLOW_VERSION}"
    docker push "apache/airflow:${INSTALL_AIRFLOW_VERSION}"
fi
