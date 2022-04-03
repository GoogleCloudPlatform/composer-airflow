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
Global constants that are used by all other Breeze components.
"""
import os
from typing import List

from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT

# Commented this out as we are using buildkit and this vars became irrelevant
# FORCE_PULL_IMAGES = False
# CHECK_IF_BASE_PYTHON_IMAGE_UPDATED = False
FORCE_BUILD_IMAGES = False
FORCE_ANSWER_TO_QUESTION = ""
SKIP_CHECK_REMOTE_IMAGE = False
# PUSH_PYTHON_BASE_IMAGE = False

DEFAULT_PYTHON_MAJOR_MINOR_VERSION = '3.7'
DEFAULT_BACKEND = 'sqlite'

# Checked before putting in build cache
ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS = ['3.7', '3.8', '3.9', '3.10']
ALLOWED_BACKENDS = ['sqlite', 'mysql', 'postgres', 'mssql']
ALLOWED_INTEGRATIONS = [
    'cassandra',
    'kerberos',
    'mongo',
    'openldap',
    'pinot',
    'rabbitmq',
    'redis',
    'statsd',
    'trino',
    'all',
]
ALLOWED_KUBERNETES_MODES = ['image']
ALLOWED_KUBERNETES_VERSIONS = ['v1.23.4', 'v1.22.7', 'v1.21.10', 'v1.20.15']
ALLOWED_KIND_VERSIONS = ['v0.12.0']
ALLOWED_HELM_VERSIONS = ['v3.6.3']
ALLOWED_EXECUTORS = ['KubernetesExecutor', 'CeleryExecutor', 'LocalExecutor', 'CeleryKubernetesExecutor']
ALLOWED_KIND_OPERATIONS = ['start', 'stop', 'restart', 'status', 'deploy', 'test', 'shell', 'k9s']
ALLOWED_GENERATE_CONSTRAINTS_MODES = ['source-providers', 'pypi-providers', 'no-providers']

MOUNT_SELECTED = "selected"
MOUNT_ALL = "all"
MOUNT_NONE = "none"

ALLOWED_MOUNT_OPTIONS = [MOUNT_SELECTED, MOUNT_ALL, MOUNT_NONE]
ALLOWED_POSTGRES_VERSIONS = ['10', '11', '12', '13']
ALLOWED_MYSQL_VERSIONS = ['5.7', '8']
ALLOWED_MSSQL_VERSIONS = ['2017-latest', '2019-latest']
ALLOWED_TEST_TYPES = [
    'All',
    'Always',
    'Core',
    'Providers',
    'API',
    'CLI',
    'Integration',
    'Other',
    'WWW',
    'Postgres',
    'MySQL',
    'Helm',
    'Quarantined',
]
ALLOWED_PACKAGE_FORMATS = ['both', 'sdist', 'wheel']
ALLOWED_INSTALLATION_METHODS = ['.', 'apache-airflow']
ALLOWED_DEBIAN_VERSIONS = ['buster', 'bullseye']
ALLOWED_BUILD_CACHE = ["pulled", "local", "disabled"]
ALLOWED_PLATFORMS = ["linux/amd64", "linux/arm64", "linux/amd64,linux/arm64"]

PARAM_NAME_DESCRIPTION = {
    "BACKEND": "backend",
    "MYSQL_VERSION": "Mysql version",
    "KUBERNETES_MODE": "Kubernetes mode",
    "KUBERNETES_VERSION": "Kubernetes version",
    "KIND_VERSION": "KinD version",
    "HELM_VERSION": "Helm version",
    "EXECUTOR": "Executors",
    "POSTGRES_VERSION": "Postgres version",
    "MSSQL_VERSION": "MSSql version",
}

PARAM_NAME_FLAG = {
    "BACKEND": "--backend",
    "MYSQL_VERSION": "--mysql-version",
    "KUBERNETES_MODE": "--kubernetes-mode",
    "KUBERNETES_VERSION": "--kubernetes-version",
    "KIND_VERSION": "--kind-version",
    "HELM_VERSION": "--helm-version",
    "EXECUTOR": "--executor",
    "POSTGRES_VERSION": "--postgres-version",
    "MSSQL_VERSION": "--mssql-version",
}

EXCLUDE_DOCS_PACKAGE_FOLDER = [
    'exts',
    'integration-logos',
    'rtd-deprecation',
    '_build',
    '_doctrees',
    '_inventory_cache',
]


def get_available_packages() -> List[str]:
    docs_path_content = (AIRFLOW_SOURCES_ROOT / 'docs').glob('*/')
    available_packages = [x.name for x in docs_path_content if x.is_dir()]
    return list(set(available_packages) - set(EXCLUDE_DOCS_PACKAGE_FOLDER))


# Initialise base variables
DOCKER_DEFAULT_PLATFORM = f"linux/{os.uname().machine}"
DOCKER_BUILDKIT = 1

SSH_PORT = "12322"
WEBSERVER_HOST_PORT = "28080"
POSTGRES_HOST_PORT = "25433"
MYSQL_HOST_PORT = "23306"
MSSQL_HOST_PORT = "21433"
FLOWER_HOST_PORT = "25555"
REDIS_HOST_PORT = "26379"

SQLITE_URL = "sqlite:////root/airflow/airflow.db"
PYTHONDONTWRITEBYTECODE = True

PRODUCTION_IMAGE = False
ALL_PYTHON_MAJOR_MINOR_VERSIONS = ['3.7', '3.8', '3.9', '3.10']
CURRENT_PYTHON_MAJOR_MINOR_VERSIONS = ['3.7', '3.8', '3.9', '3.10']
CURRENT_POSTGRES_VERSIONS = ['10', '11', '12', '13']
CURRENT_MYSQL_VERSIONS = ['5.7', '8']
CURRENT_MSSQL_VERSIONS = ['2017-latest', '2019-latest']
POSTGRES_VERSION = CURRENT_POSTGRES_VERSIONS[0]
MYSQL_VERSION = CURRENT_MYSQL_VERSIONS[0]
MSSQL_VERSION = CURRENT_MSSQL_VERSIONS[0]
DB_RESET = False
START_AIRFLOW = "false"
LOAD_EXAMPLES = False
LOAD_DEFAULT_CONNECTIONS = False
PRESERVE_VOLUMES = False
CLEANUP_DOCKER_CONTEXT_FILES = False
INIT_SCRIPT_FILE = ""
DRY_RUN_DOCKER = False
INSTALL_AIRFLOW_VERSION = ""
SQLITE_URL = "sqlite:////root/airflow/airflow.db"


def get_airflow_version():
    airflow_setup_file = AIRFLOW_SOURCES_ROOT / 'setup.py'
    with open(airflow_setup_file) as setup_file:
        for line in setup_file.readlines():
            if "version =" in line:
                return line.split()[2][1:-1]


def get_airflow_extras():
    airflow_dockerfile = AIRFLOW_SOURCES_ROOT / 'Dockerfile'
    with open(airflow_dockerfile) as dockerfile:
        for line in dockerfile.readlines():
            if "ARG AIRFLOW_EXTRAS=" in line:
                line = line.split('=')[1].strip()
                return line.replace('"', '')


# Initialize integrations
AVAILABLE_INTEGRATIONS = [
    'cassandra',
    'kerberos',
    'mongo',
    'openldap',
    'pinot',
    'rabbitmq',
    'redis',
    'statsd',
    'trino',
]
ENABLED_INTEGRATIONS = ""
# Initialize files for rebuild check
FILES_FOR_REBUILD_CHECK = [
    'setup.py',
    'setup.cfg',
    'Dockerfile.ci',
    '.dockerignore',
    'scripts/docker/compile_www_assets.sh',
    'scripts/docker/common.sh',
    'scripts/docker/install_additional_dependencies.sh',
    'scripts/docker/install_airflow.sh',
    'scripts/docker/install_airflow_dependencies_from_branch_tip.sh',
    'scripts/docker/install_from_docker_context_files.sh',
    'scripts/docker/install_mysql.sh',
    'airflow/www/package.json',
    'airflow/www/yarn.lock',
    'airflow/www/webpack.config.js',
    'airflow/ui/package.json',
    'airflow/ui/yarn.lock',
]

ENABLED_SYSTEMS = ""

CURRENT_KUBERNETES_MODES = ['image']
CURRENT_KUBERNETES_VERSIONS = ['v1.23.4', 'v1.22.7', 'v1.21.10', 'v1.20.15']
CURRENT_KIND_VERSIONS = ['v0.12.0']
CURRENT_HELM_VERSIONS = ['v3.6.3']
CURRENT_EXECUTORS = ['KubernetesExecutor']

DEFAULT_KUBERNETES_MODES = CURRENT_KUBERNETES_MODES[0]
DEFAULT_KUBERNETES_VERSIONS = CURRENT_KUBERNETES_VERSIONS[0]
DEFAULT_KIND_VERSIONS = CURRENT_KIND_VERSIONS[0]
DEFAULT_HELM_VERSIONS = CURRENT_HELM_VERSIONS[0]
DEFAULT_EXECUTOR = CURRENT_EXECUTORS[0]

# Initialize image build variables - Have to check if this has to go to ci dataclass
SKIP_TWINE_CHECK = ""
USE_AIRFLOW_VERSION = ""
GITHUB_ACTIONS = ""

ISSUE_ID = ""
NUM_RUNS = ""

# Initialize package variables
PACKAGE_FORMAT = "wheel"
VERSION_SUFFIX_FOR_SVN = ""
VERSION_SUFFIX_FOR_PYPI = ""

MIN_DOCKER_VERSION = "20.10.0"
MIN_DOCKER_COMPOSE_VERSION = "1.29.0"

AIRFLOW_SOURCES_FROM = "."
AIRFLOW_SOURCES_TO = "/opt/airflow"
AIRFLOW_SOURCES_WWW_FROM = "./airflow/www"
AIRFLOW_SOURCES_WWW_TO = "/opt/airflow/airflow/www"
