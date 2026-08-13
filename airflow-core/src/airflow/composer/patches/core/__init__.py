"""
Composer core patch.

Changes:
- introduce utils.py module with helper functions
- introduce Composer airflow_local_settings.py file
- apply all patches from monkey_patching/ folder of each Composer patch
- fail tasks if triggerer is not enabled
- bump default value of the `timeout_worker_healthcheck` parameter of uvicorn.run method
- add retries for parsing DAG during task execution to overcome DAG files synchronization issue
"""
