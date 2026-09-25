import json
from unittest.mock import MagicMock

import dagster as dg
import pytest
from backend.defs.jobs.test_plan import test_plan_job
from backend.defs.resources import JiraClient
from backend.defs.sensors.jira_sensor import (
    _format_jql_date,
    _parse_jira_datetime,
    jira_new_items_sensor,
)


def test_test_plan_job_asset_selection():
    """Verify that test_plan_job includes all expected pipeline assets."""
    from backend.definitions import defs

    definitions = defs()
    resolved_job = definitions.resolve_job_def("test_plan_job")
    assert resolved_job is not None

    asset_keys = {
        key.to_user_string()
        for key in resolved_job.asset_layer.selected_asset_keys
    }
    assert asset_keys == {
        "jira_issue",
        "jira_issue_epic",
        "epic_requirements",
        "processed_jira_issue",
        "test_plan",
        "test_cases",
    }


def test_test_plan_job_run_config(monkeypatch):
    """Verify that run_config with jira_id is valid for test_plan_job."""
    monkeypatch.setenv("BASE_URL", "https://example.atlassian.net")
    monkeypatch.setenv("ATLASSIAN_EMAIL", "user@example.com")
    monkeypatch.setenv("ATLASSIAN_API_KEY", "dummy-api-key")
    monkeypatch.setenv("GOOGLE_API_KEY", "dummy-google-key")

    from backend.definitions import defs

    definitions = defs()
    resolved_job = definitions.resolve_job_def("test_plan_job")

    valid_config = {
        "ops": {
            "jira_issue": {
                "config": {
                    "jira_id": "SIS-1369",
                }
            }
        }
    }
    res = dg.validate_run_config(resolved_job, run_config=valid_config)
    assert res is not None


def test_parse_and_format_jira_datetime():
    """Verify parsing ISO datetime from Jira and formatting to JQL."""
    iso_date = "2026-09-25T07:49:48.826-0500"
    dt = _parse_jira_datetime(iso_date)
    assert dt is not None
    assert dt.year == 2026
    assert dt.month == 9
    assert dt.day == 25

    jql_date = _format_jql_date(dt)
    assert jql_date == "2026-09-25 07:49"


def test_jira_new_items_sensor_emits_run_requests(monkeypatch):
    """Verify sensor detects new issues and yields corresponding RunRequests and updates cursor."""
    monkeypatch.setenv("ATLASSIAN_SPACE", "SIS")

    mock_jira_client = MagicMock()
    mock_jira_client.search_issues.return_value = {
        "issues": [
            {
                "key": "SIS-101",
                "fields": {
                    "summary": "Implement Auth Middleware",
                    "created": "2026-09-20T10:00:00.000+0000",
                    "issuetype": {"name": "Story"},
                    "status": {"name": "To Do"},
                },
            },
            {
                "key": "SIS-102",
                "fields": {
                    "summary": "Fix Token Refresh Bug",
                    "created": "2026-09-21T12:30:00.000+0000",
                    "issuetype": {"name": "Bug"},
                    "status": {"name": "In Progress"},
                },
            },
        ]
    }

    context = dg.build_sensor_context(
        cursor=None,
        resources={"jira_client": mock_jira_client},
    )

    result = jira_new_items_sensor(context)

    # Verify JQL query passed to Jira client includes created in last 7 days and project filter
    mock_jira_client.search_issues.assert_called_once()
    called_jql = mock_jira_client.search_issues.call_args.kwargs["jql"]
    assert "created >= -7d" in called_jql
    assert "project = 'SIS'" in called_jql
    assert called_jql.endswith("ORDER BY created ASC")

    assert isinstance(result, list)
    assert len(result) == 2

    # Check first RunRequest
    req1 = result[0]
    assert isinstance(req1, dg.RunRequest)
    assert req1.run_key == "jira_test_plan_SIS-101"
    assert req1.run_config == {
        "ops": {
            "jira_issue": {
                "config": {
                    "jira_id": "SIS-101",
                }
            }
        }
    }
    assert req1.tags["jira_id"] == "SIS-101"
    assert req1.tags["summary"] == "Implement Auth Middleware"
    assert req1.tags["issue_type"] == "Story"

    # Check second RunRequest
    req2 = result[1]
    assert isinstance(req2, dg.RunRequest)
    assert req2.run_key == "jira_test_plan_SIS-102"
    assert req2.tags["jira_id"] == "SIS-102"

    # Check cursor update
    assert context.cursor is not None
    cursor_state = json.loads(context.cursor)
    assert "SIS-101" in cursor_state["seen_keys"]
    assert "SIS-102" in cursor_state["seen_keys"]
    assert cursor_state["last_created"] == "2026-09-21T12:30:00.000+0000"


def test_jira_new_items_sensor_skips_seen_issues(monkeypatch):
    """Verify sensor ignores already-seen issues and returns SkipReason."""
    monkeypatch.setenv("ATLASSIAN_SPACE", "SIS")

    initial_cursor = json.dumps(
        {
            "last_created": "2026-09-21T12:30:00.000+0000",
            "seen_keys": ["SIS-101", "SIS-102"],
        }
    )

    mock_jira_client = MagicMock()
    mock_jira_client.search_issues.return_value = {
        "issues": [
            {
                "key": "SIS-101",
                "fields": {
                    "summary": "Implement Auth Middleware",
                    "created": "2026-09-20T10:00:00.000+0000",
                },
            },
            {
                "key": "SIS-102",
                "fields": {
                    "summary": "Fix Token Refresh Bug",
                    "created": "2026-09-21T12:30:00.000+0000",
                },
            },
        ]
    }

    context = dg.build_sensor_context(
        cursor=initial_cursor,
        resources={"jira_client": mock_jira_client},
    )

    result = jira_new_items_sensor(context)
    assert isinstance(result, dg.SkipReason)


def test_jira_new_items_sensor_handles_empty_response(monkeypatch):
    """Verify sensor handles empty Jira search response gracefully."""
    monkeypatch.setenv("ATLASSIAN_SPACE", "SIS")

    mock_jira_client = MagicMock()
    mock_jira_client.search_issues.return_value = {"issues": []}

    context = dg.build_sensor_context(
        cursor=None,
        resources={"jira_client": mock_jira_client},
    )

    result = jira_new_items_sensor(context)
    assert isinstance(result, dg.SkipReason)


def test_jira_new_items_sensor_handles_api_exception(monkeypatch):
    """Verify sensor returns SkipReason when Jira API call raises an exception."""
    monkeypatch.setenv("ATLASSIAN_SPACE", "SIS")

    mock_jira_client = MagicMock()
    mock_jira_client.search_issues.side_effect = Exception("Connection error to Jira")

    context = dg.build_sensor_context(
        cursor=None,
        resources={"jira_client": mock_jira_client},
    )

    result = jira_new_items_sensor(context)
    assert isinstance(result, dg.SkipReason)
    assert "Failed to query Jira" in result.skip_message


def test_jira_new_items_sensor_multi_tick_progression(monkeypatch):
    """Verify cursor correctly accumulates seen keys across successive evaluation ticks."""
    monkeypatch.setenv("ATLASSIAN_SPACE", "SIS")

    mock_jira_client = MagicMock()

    # Tick 1: returns SIS-201
    mock_jira_client.search_issues.return_value = {
        "issues": [
            {
                "key": "SIS-201",
                "fields": {
                    "summary": "First Issue",
                    "created": "2026-09-22T08:00:00.000+0000",
                },
            }
        ]
    }

    context1 = dg.build_sensor_context(
        cursor=None,
        resources={"jira_client": mock_jira_client},
    )
    result1 = jira_new_items_sensor(context1)
    assert len(result1) == 1
    assert result1[0].run_key == "jira_test_plan_SIS-201"
    cursor_after_tick1 = context1.cursor

    # Tick 2: search returns SIS-201 and new SIS-202
    mock_jira_client.search_issues.return_value = {
        "issues": [
            {
                "key": "SIS-201",
                "fields": {
                    "summary": "First Issue",
                    "created": "2026-09-22T08:00:00.000+0000",
                },
            },
            {
                "key": "SIS-202",
                "fields": {
                    "summary": "Second Issue",
                    "created": "2026-09-23T09:00:00.000+0000",
                },
            },
        ]
    }

    context2 = dg.build_sensor_context(
        cursor=cursor_after_tick1,
        resources={"jira_client": mock_jira_client},
    )
    result2 = jira_new_items_sensor(context2)
    assert len(result2) == 1
    assert result2[0].run_key == "jira_test_plan_SIS-202"

    # Verify JQL on tick 2 combines created >= -7d, project, and cursor timestamp
    tick2_jql = mock_jira_client.search_issues.call_args.kwargs["jql"]
    assert "created >= -7d" in tick2_jql
    assert "project = 'SIS'" in tick2_jql
    assert "created >= '2026-09-22 08:00'" in tick2_jql

    cursor2_state = json.loads(context2.cursor)
    assert "SIS-201" in cursor2_state["seen_keys"]
    assert "SIS-202" in cursor2_state["seen_keys"]
    assert cursor2_state["last_created"] == "2026-09-23T09:00:00.000+0000"


def test_jira_new_items_sensor_jql_without_project_env(monkeypatch):
    """Verify JQL restricts to last 7 days even when no project env var is set."""
    monkeypatch.delenv("ATLASSIAN_SPACE", raising=False)
    monkeypatch.delenv("JIRA_PROJECT", raising=False)

    mock_jira_client = MagicMock()
    mock_jira_client.search_issues.return_value = {"issues": []}

    context = dg.build_sensor_context(
        cursor=None,
        resources={"jira_client": mock_jira_client},
    )

    jira_new_items_sensor(context)
    mock_jira_client.search_issues.assert_called_once()
    called_jql = mock_jira_client.search_issues.call_args.kwargs["jql"]
    assert called_jql == "created >= -7d ORDER BY created ASC"
