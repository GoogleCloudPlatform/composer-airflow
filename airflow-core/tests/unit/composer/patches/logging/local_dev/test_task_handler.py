from __future__ import annotations

from datetime import datetime, timezone
from itertools import chain
from unittest import mock

import pytest

from airflow.composer.patches.logging.local_dev.task_handler import (
    COMPOSER_LOG_FIELDS_TO_OMIT,
    ComposerLocalDevTaskHandler,
)
from airflow.utils.log.file_task_handler import FileTaskHandler, StructuredLogMessage

TIMESTAMP = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
COMPOSER_TI_INFO = {
    "workflow": "my_dag",
    "task-id": "my_task",
    "run-id": "manual__1",
    "map-index": "-1",
    "try-number": "1",
}
ERROR_DETAIL = [
    {
        "exc_type": "ValueError",
        "exc_value": "boom",
        "frames": [{"filename": "my_dag.py", "lineno": 25, "name": "check"}],
    }
]


@pytest.fixture
def log_handler():
    """Fixture to provide an instance of ComposerLocalDevTaskHandler with mocked super().__init__."""
    with mock.patch.object(FileTaskHandler, "__init__", return_value=None):
        handler = ComposerLocalDevTaskHandler()
    return handler


def _read(log_handler, raw_logs):
    with mock.patch.object(
        FileTaskHandler, "read", return_value=(chain(raw_logs), {"end_of_log": True})
    ) as mock_super_read:
        logs, metadata = log_handler.read(mock.MagicMock())
        logs = list(logs)
    return logs, metadata, mock_super_read


class TestComposerLocalDevTaskHandler:
    def test_omitted_fields(self):
        assert {"composer_ti_info", "composer_extra_info"} == COMPOSER_LOG_FIELDS_TO_OMIT

    @mock.patch.object(FileTaskHandler, "__init__", return_value=None)
    def test_init_passes_args_and_kwargs_to_super(self, mock_super_init):
        args = ("base_log_folder",)
        kwargs = {"max_bytes": 0}

        handler = ComposerLocalDevTaskHandler(*args, **kwargs)

        mock_super_init.assert_called_once_with(*args, **kwargs)
        assert isinstance(handler, FileTaskHandler)

    def test_read_passes_arguments_to_super_and_returns_metadata(self, log_handler):
        mock_ti = mock.MagicMock()
        with mock.patch.object(
            FileTaskHandler, "read", return_value=(chain([]), {"end_of_log": True, "log_pos": 5})
        ) as mock_super_read:
            logs, metadata = log_handler.read(mock_ti, try_number=2, metadata={"log_pos": 1})

            assert list(logs) == []

        mock_super_read.assert_called_once_with(mock_ti, 2, {"log_pos": 1})
        assert metadata == {"end_of_log": True, "log_pos": 5}

    def test_read_returns_lazy_stream(self, log_handler):
        def raw_stream():
            yield StructuredLogMessage(event="first", timestamp=TIMESTAMP)
            raise AssertionError("Stream must not be consumed eagerly")

        with mock.patch.object(FileTaskHandler, "read", return_value=(raw_stream(), {"end_of_log": True})):
            logs, _ = log_handler.read(mock.MagicMock())
            assert next(logs).event == "first"

    @pytest.mark.parametrize("field", sorted(COMPOSER_LOG_FIELDS_TO_OMIT))
    def test_read_removes_composer_field(self, log_handler, field):
        raw = StructuredLogMessage(event="message", timestamp=TIMESTAMP, **{field: COMPOSER_TI_INFO})

        logs, _, _ = _read(log_handler, [raw])

        assert len(logs) == 1
        assert field not in logs[0].model_dump()
        assert logs[0].event == "message"
        assert logs[0].timestamp == TIMESTAMP

    def test_read_keeps_level_source_and_task_fields(self, log_handler):
        raw = StructuredLogMessage(
            event="simulated Cloud Run line 0",
            timestamp=TIMESTAMP,
            level="info",
            logger="my_logger",
            filename="my_dag.py",
            lineno=21,
            dag_id="my_dag",
            task_id="my_task",
            run_id="manual__1",
            ti_id="0199",
            try_number=1,
            map_index=-1,
            composer_ti_info=COMPOSER_TI_INFO,
        )

        logs, _, _ = _read(log_handler, [raw])

        assert logs[0].model_dump() == {
            "event": "simulated Cloud Run line 0",
            "timestamp": TIMESTAMP,
            "level": "info",
            "logger": "my_logger",
            "filename": "my_dag.py",
            "lineno": 21,
            "dag_id": "my_dag",
            "task_id": "my_task",
            "run_id": "manual__1",
            "ti_id": "0199",
            "try_number": 1,
            "map_index": -1,
        }

    def test_read_keeps_exception_details(self, log_handler):
        raw = StructuredLogMessage(
            event="Task failed with exception",
            timestamp=TIMESTAMP,
            level="error",
            error_detail=ERROR_DETAIL,
            composer_ti_info=COMPOSER_TI_INFO,
        )

        logs, _, _ = _read(log_handler, [raw])

        assert logs[0].level == "error"
        assert logs[0].error_detail == ERROR_DETAIL
        assert "composer_ti_info" not in logs[0].model_dump()

    @pytest.mark.parametrize(
        "event",
        [
            "user text with dag_id=my_dag inside",
            "composer_ti_info={'workflow': 'x'} is part of the user message",
            str({"dag_id": "d1", "task_id": "t1", "value": 1}),
            "",
        ],
    )
    def test_read_never_modifies_event(self, log_handler, event):
        raw = StructuredLogMessage(event=event, timestamp=TIMESTAMP, composer_ti_info=COMPOSER_TI_INFO)

        logs, _, _ = _read(log_handler, [raw])

        assert logs[0].event == event

    def test_read_keeps_non_string_event(self, log_handler):
        # Equivalent of a task logging a dictionary that was not converted to a string.
        logged_dict = {"dag_id": "d1", "value": 1}
        raw = StructuredLogMessage.model_construct(
            event=logged_dict, timestamp=TIMESTAMP, composer_ti_info=COMPOSER_TI_INFO
        )

        logs, _, _ = _read(log_handler, [raw])

        assert logs[0].event == logged_dict
        assert "composer_ti_info" not in logs[0].model_dump()

    def test_read_returns_same_object_when_nothing_to_strip(self, log_handler):
        plain = StructuredLogMessage(event="plain", timestamp=TIMESTAMP)
        with_extras = StructuredLogMessage(event="extras", timestamp=TIMESTAMP, level="info")

        logs, _, _ = _read(log_handler, [plain, with_extras])

        assert logs[0] is plain
        assert logs[1] is with_extras

    def test_read_keeps_message_without_timestamp(self, log_handler):
        raw = StructuredLogMessage(event="::group::Log message source details", composer_ti_info={})

        logs, _, _ = _read(log_handler, [raw])

        assert logs[0].model_dump() == {"event": "::group::Log message source details", "timestamp": None}

    def test_read_passes_through_non_structured_items(self, log_handler):
        raw_logs = ["raw string", ["a", "b"], {"raw": "dict"}, None]

        logs, _, _ = _read(log_handler, raw_logs)

        assert logs == raw_logs

    def test_read_mixed_stream_keeps_order(self, log_handler):
        raw_logs = [
            StructuredLogMessage(event=f"line {i}", timestamp=TIMESTAMP, composer_ti_info=COMPOSER_TI_INFO)
            for i in range(5)
        ]

        logs, _, _ = _read(log_handler, raw_logs)

        assert [log.event for log in logs] == [f"line {i}" for i in range(5)]
        assert all("composer_ti_info" not in log.model_dump() for log in logs)

    def test_stripped_message_serializes_like_original_without_composer_fields(self, log_handler):
        raw = StructuredLogMessage.model_validate_json(
            '{"timestamp":"2026-10-28T12:25:31.935991Z","level":"warning","event":"warning line",'
            '"logger":"my_logger","composer_ti_info":{"workflow":"my_dag"},"filename":"my_dag.py","lineno":24}'
        )

        logs, _, _ = _read(log_handler, [raw])

        expected = raw.model_dump(mode="json")
        del expected["composer_ti_info"]
        assert logs[0].model_dump(mode="json") == expected
