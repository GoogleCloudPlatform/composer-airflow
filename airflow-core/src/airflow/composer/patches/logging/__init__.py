"""
Composer logging patch.

Changes:
- apply custom format for supervisor logs
- annotate supervisor logs with Composer labels
- patch task runner log processors
- patch default uvicorn handler
- filter warnings that are not relevant for customer/Composer
- enable logging slow callbacks in Airflow triggers
- use custom log handler to read logs from Cloud Logging
- write DAG files processing logs to /dev/null
"""
