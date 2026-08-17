#
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import functools
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING

from airflow.composer.patches.core.email_backend import (
    _DEFAULT_EMAIL_BACKEND,
    _ErrorEmailNotifier,
    _LegacyEmailBackendNotifier,
)
from airflow.configuration import conf
from airflow.sdk.execution_time import task_runner

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger as Logger

    from airflow.sdk.bases.operator import BaseOperator
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.definitions.mappedoperator import MappedOperator
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

logger = logging.getLogger(__name__)


def patch():
    task_runner.parse = _composer_task_runner_parse(task_runner.parse)
    task_runner._send_error_email_notification = _composer_send_error_email_notification


def _composer_task_runner_parse(f):
    """
    Handle gracefully DAG parsing failure during task execution.

    This patch will assure that DAG parsing is retried if it fails. This is need to gracefully handle scenario
    when DAG file(s) are not yet synced to worker (resulting in DAG parsing failure), while they were already
    synced to DAG processor and scheduler, and DAG was already scheduled for execution.

    DAG parsing will be done in a cycle until it succeeds or times out (time out controlled by
    [core]wait_dag_not_found_timeout Airflow configuration property).
    """

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        wait_dag_not_found_timeout = conf.getint("core", "wait_dag_not_found_timeout", fallback=0)
        start_time = time.time()

        while True:
            time_passed_before_parse = time.time() - start_time

            try:
                result = f(*args, **kwargs)
            except SystemExit:
                # SystemExit exception is raised ("exit" method is called) when DAG or task was not found
                # after parsing DAG file(s).
                # It is expected to happen in case DAG file(s) are not yet synced to worker, while they were
                # already synced to DAG processor and scheduler, and DAG was already scheduled for execution.
                # In this case, we catch exception and will retry in the next iteration.
                pass
            else:
                # If there is no exception, then break the loop and return result.
                break

            if time_passed_before_parse > wait_dag_not_found_timeout:
                raise SystemExit(1)

            sleep_time = 5
            logger.warning(
                "DAG or task is not found in loaded DAG bag. Retrying after %s seconds.", sleep_time
            )
            time.sleep(sleep_time)

        return result

    return wrapper


# TODO(Internal bug): remove this patch when preparing Airflow 3.3.1+
def _composer_send_error_email_notification(
    task: BaseOperator | MappedOperator,
    ti: RuntimeTaskInstance,
    context: Context,
    error: BaseException | str | None,
    log: Logger,
) -> None:
    """
    Send email notification for task errors through the configured email backend.

    Monkey patched implementation of _send_error_email_notification that routes
    through [email] email_backend before falling back to SMTP. Backported from PR 69877.

    A non-default ``[email] email_backend`` (an SES, SendGrid or org-internal callable with the
    ``airflow.utils.email.send_email`` signature) is wrapped in
    :class:`~airflow.sdk.execution_time.email_backend._LegacyEmailBackendNotifier`; otherwise the
    default :class:`~airflow.providers.smtp.notifications.smtp.SmtpNotifier` is used.

    Both the worker task-runner path (:func:`finalize`) and the DAG-processor callback path
    (``_execute_email_callbacks``) funnel through this function, so the resolved backend is used
    consistently regardless of how the task failed.
    """
    if not task.email:
        return

    email_backend = conf.get("email", "email_backend", fallback=_DEFAULT_EMAIL_BACKEND)
    notifier_description = "SmtpNotifier"

    if email_backend and email_backend != _DEFAULT_EMAIL_BACKEND:
        notifier_class: _ErrorEmailNotifier = _LegacyEmailBackendNotifier
        notifier_description = f"configured email_backend {email_backend!r}"
    else:
        try:
            from airflow.providers.smtp.notifications.smtp import SmtpNotifier
        except ImportError:
            log.error(
                "Failed to send task failure or retry email notification: "
                "`apache-airflow-providers-smtp` is not installed. "
                "Install this provider to enable email notifications."
            )
            return
        notifier_class = SmtpNotifier

    subject_template_file = conf.get("email", "subject_template", fallback=None)

    # Read the template file if configured
    if subject_template_file and Path(subject_template_file).exists():
        subject = Path(subject_template_file).read_text()
    else:
        # Fallback to default
        subject = "Airflow alert: {{ti}}"

    html_content_template_file = conf.get("email", "html_content_template", fallback=None)

    # Read the template file if configured
    if html_content_template_file and Path(html_content_template_file).exists():
        html_content = Path(html_content_template_file).read_text()
    else:
        # Fallback to default
        # For reporting purposes, we report based on 1-indexed,
        # not 0-indexed lists (i.e. Try 1 instead of Try 0 for the first attempt).
        html_content = (
            "Try {{try_number}} out of {{max_tries + 1}}<br>"
            "Exception:<br>{{exception_html}}<br>"
            'Log: <a href="{{ti.log_url}}">Link</a><br>'
            "Host: {{ti.hostname}}<br>"
            'Mark success: <a href="{{ti.mark_success_url}}">Link</a><br>'
        )

    # Add exception_html to context for template rendering
    import html

    exception_html = html.escape(str(error)).replace("\n", "<br>")
    additional_context = {
        "exception": error,
        "exception_html": exception_html,
        "try_number": ti.try_number,
        "max_tries": ti.max_tries,
    }
    email_context = {**context, **additional_context}
    to_emails = task.email
    if not to_emails:
        return

    try:
        notifier = notifier_class(
            to=to_emails,
            subject=subject,
            html_content=html_content,
            from_email=conf.get("email", "from_email", fallback=None),
        )
        notifier(email_context)
    except Exception:
        log.exception("Failed to send email notification via %s", notifier_description)
