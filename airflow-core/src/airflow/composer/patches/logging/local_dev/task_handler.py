from __future__ import annotations

from airflow.utils.log.file_task_handler import FileTaskHandler, StructuredLogMessage

# Fields added to every log record by Composer to label logs in Cloud Logging. In Composer on GCP they are
# stripped before logs are shown in the UI. In composer-local-dev logs are read from local files, so the UI
# would render them as "key=value" next to every log line.
COMPOSER_LOG_FIELDS_TO_OMIT = frozenset({"composer_ti_info", "composer_extra_info"})


class ComposerLocalDevTaskHandler(FileTaskHandler):
    """
    Task log handler that strips Composer log labels from task logs before UI rendering.

    Only Composer-specific fields are removed. The log message itself and all other fields (level, logger,
    source location, exception details, user-provided fields) are kept, so logs are rendered the same way as
    in Airflow without Composer patches.

    This class is specifically for the Composer Local Development environment.
    """

    def read(self, task_instance, try_number=None, metadata=None):
        logs, metadata = super().read(task_instance, try_number, metadata)
        return (self._strip_composer_fields(log) for log in logs), metadata

    @staticmethod
    def _strip_composer_fields(log):
        if not isinstance(log, StructuredLogMessage) or not log.model_extra:
            return log
        if COMPOSER_LOG_FIELDS_TO_OMIT.isdisjoint(log.model_extra):
            return log
        fields = {
            key: value
            for key, value in log.model_dump(exclude_unset=True).items()
            if key not in COMPOSER_LOG_FIELDS_TO_OMIT
        }
        return StructuredLogMessage.model_construct(**fields)
