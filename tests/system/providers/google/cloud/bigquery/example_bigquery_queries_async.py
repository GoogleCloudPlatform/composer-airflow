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
Example Airflow DAG for Google BigQuery service.
Uses Async version of the Big Query Operators

"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckAsyncOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryGetDataAsyncOperator,
    BigQueryInsertJobAsyncOperator,
    BigQueryIntervalCheckAsyncOperator,
    BigQueryValueCheckAsyncOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.getenv("SYSTEM_TESTS_GCP_PROJECT")
DAG_ID = "bigquery_queries_async"
DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
LOCATION = "us"
EXECUTION_TIMEOUT = 6

TABLE_1 = "table1"
TABLE_2 = "table2"

SCHEMA = [
    {"name": "value", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ds", "type": "STRING", "mode": "NULLABLE"},
]

DATASET = DATASET_NAME
INSERT_DATE = datetime.now().strftime("%Y-%m-%d")
INSERT_ROWS_QUERY = (
    f"INSERT {DATASET}.{TABLE_1} VALUES "
    f"(42, 'monthy python', '{INSERT_DATE}'), "
    f"(42, 'fishy fish', '{INSERT_DATE}');"
)

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}

with DAG(
    dag_id="example_async_bigquery_queries_async",
    schedule=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "bigquery"],
    user_defined_macros={"DATASET": DATASET, "TABLE": TABLE_1},
) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=DATASET,
        location=LOCATION,
    )

    create_table_1 = BigQueryCreateEmptyTableOperator(
        task_id="create_table_1",
        dataset_id=DATASET,
        table_id=TABLE_1,
        schema_fields=SCHEMA,
        location=LOCATION,
    )

    create_dataset >> create_table_1

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset", dataset_id=DATASET, delete_contents=True, trigger_rule=TriggerRule.ALL_DONE
    )

    # [START howto_operator_bigquery_insert_job_async]
    insert_query_job = BigQueryInsertJobAsyncOperator(
        task_id="insert_query_job",
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,
            }
        },
        location=LOCATION,
    )
    # [END howto_operator_bigquery_insert_job_async]

    # [START howto_operator_bigquery_select_job_async]
    select_query_job = BigQueryInsertJobAsyncOperator(
        task_id="select_query_job",
        configuration={
            "query": {
                "query": "{% include 'example_bigquery_query.sql' %}",
                "useLegacySql": False,
            }
        },
        location=LOCATION,
    )
    # [END howto_operator_bigquery_select_job_async]

    # [START howto_operator_bigquery_value_check_async]
    check_value = BigQueryValueCheckAsyncOperator(
        task_id="check_value",
        sql=f"SELECT COUNT(*) FROM {DATASET}.{TABLE_1}",
        pass_value=2,
        use_legacy_sql=False,
        location=LOCATION,
    )
    # [END howto_operator_bigquery_value_check_async]

    # [START howto_operator_bigquery_interval_check_async]
    check_interval = BigQueryIntervalCheckAsyncOperator(
        task_id="check_interval",
        table=f"{DATASET}.{TABLE_1}",
        days_back=1,
        metrics_thresholds={"COUNT(*)": 1.5},
        use_legacy_sql=False,
        location=LOCATION,
    )
    # [END howto_operator_bigquery_interval_check_async]

    # [START howto_operator_bigquery_multi_query_async]
    bigquery_execute_multi_query = BigQueryInsertJobAsyncOperator(
        task_id="execute_multi_query",
        configuration={
            "query": {
                "query": [
                    f"SELECT * FROM {DATASET}.{TABLE_2}",
                    f"SELECT COUNT(*) FROM {DATASET}.{TABLE_2}",
                ],
                "useLegacySql": False,
            }
        },
        location=LOCATION,
    )
    # [END howto_operator_bigquery_multi_query_async]

    # [START howto_operator_bigquery_get_data_async]
    get_data = BigQueryGetDataAsyncOperator(
        task_id="get_data",
        dataset_id=DATASET,
        table_id=TABLE_1,
        max_results=10,
        selected_fields="value,name",
        location=LOCATION,
    )
    # [END howto_operator_bigquery_get_data_async]

    get_data_result = BashOperator(
        task_id="get_data_result",
        bash_command=f"echo {get_data.output}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # [START howto_operator_bigquery_check_async]
    check_count = BigQueryCheckAsyncOperator(
        task_id="check_count",
        sql=f"SELECT COUNT(*) FROM {DATASET}.{TABLE_1}",
        use_legacy_sql=False,
        location=LOCATION,
    )
    # [END howto_operator_bigquery_check_async]

    # [START howto_operator_bigquery_execute_query_save_async]
    execute_query_save = BigQueryInsertJobAsyncOperator(
        task_id="execute_query_save",
        configuration={
            "query": {
                "query": f"SELECT * FROM {DATASET}.{TABLE_1}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET,
                    "tableId": TABLE_2,
                },
            }
        },
        location=LOCATION,
    )
    # [END howto_operator_bigquery_execute_query_save_async]

    execute_long_running_query = BigQueryInsertJobAsyncOperator(
        task_id="execute_long_running_query",
        configuration={
            "query": {
                "query": f"""DECLARE success BOOL;
    DECLARE size_bytes INT64;
    DECLARE row_count INT64;
    DECLARE DELAY_TIME DATETIME;
    DECLARE WAIT STRING;
    SET success = FALSE;

    SELECT row_count = (SELECT row_count FROM {DATASET}.__TABLES__ WHERE table_id='NON_EXISTING_TABLE');
    IF row_count > 0  THEN
        SELECT 'Table Exists!' as message, retry_count as retries;
        SET success = TRUE;
    ELSE
        SELECT 'Table does not exist' as message, row_count;
        SET WAIT = 'TRUE';
        SET DELAY_TIME = DATETIME_ADD(CURRENT_DATETIME,INTERVAL 1 MINUTE);
        WHILE WAIT = 'TRUE' DO
          IF (DELAY_TIME < CURRENT_DATETIME) THEN
              SET WAIT = 'FALSE';
          END IF;
        END WHILE;
    END IF;""",
                "useLegacySql": False,
            }
        },
        location=LOCATION,
    )

    end = EmptyOperator(task_id="end")

    create_table_1 >> insert_query_job >> select_query_job >> check_count
    insert_query_job >> get_data >> get_data_result
    insert_query_job >> execute_query_save >> bigquery_execute_multi_query
    insert_query_job >> execute_long_running_query >> check_value >> check_interval
    [check_count, check_interval, bigquery_execute_multi_query, get_data_result] >> delete_dataset
    [check_count, check_interval, bigquery_execute_multi_query, get_data_result, delete_dataset] >> end


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
