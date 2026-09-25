import json
import os
import re
from datetime import datetime
from typing import Any

import dagster as dg

from backend.defs.jobs.test_plan import test_plan_job
from backend.defs.resources import JiraClient


def _parse_jira_datetime(date_str: str) -> datetime | None:
    """Parses Jira ISO-8601 timestamp string into a datetime object."""
    if not date_str:
        return None
    try:
        clean_str = date_str.replace("Z", "+00:00")
        if re.search(r"[+-]\d{4}$", clean_str):
            clean_str = clean_str[:-2] + ":" + clean_str[-2:]
        return datetime.fromisoformat(clean_str)
    except (ValueError, TypeError):
        return None


def _format_jql_date(dt: datetime) -> str:
    """Formats datetime object to Jira JQL query format (YYYY-MM-DD HH:mm)."""
    return dt.strftime("%Y-%m-%d %H:%M")


@dg.sensor(
    job=test_plan_job,
    minimum_interval_seconds=60,
    description="Polls Jira for issues created in the last 7 days and triggers the test plan and full test cases generation pipeline.",
)
def jira_new_items_sensor(
    context: dg.SensorEvaluationContext,
    jira_client: JiraClient,
):
    """Dagster sensor that monitors Jira for newly created issues within the last 7 days

    and emits RunRequests to materialize the full test plan and test cases pipeline assets for each new issue.
    """
    cursor_data: dict[str, Any] = {}
    if context.cursor:
        try:
            cursor_data = json.loads(context.cursor)
        except json.JSONDecodeError:
            cursor_data = {"last_created": context.cursor, "seen_keys": []}

    last_created_str: str | None = cursor_data.get("last_created")
    seen_keys: list[str] = cursor_data.get("seen_keys", [])
    seen_keys_set: set[str] = set(seen_keys)

    target_project = os.getenv("ATLASSIAN_SPACE") or os.getenv("JIRA_PROJECT")

    query_parts: list[str] = ["created >= -7d"]
    if target_project:
        query_parts.append(f"project = '{target_project}'")

    if last_created_str:
        dt = _parse_jira_datetime(last_created_str)
        if dt:
            jql_date = _format_jql_date(dt)
            query_parts.append(f"created >= '{jql_date}'")

    jql = " AND ".join(query_parts) + " ORDER BY created ASC"

    context.log.info(f"Checking Jira for new items with JQL: {jql}")

    try:
        search_res = jira_client.search_issues(
            jql=jql,
            fields=["key", "summary", "created", "updated", "status", "issuetype"],
            max_results=20,
        )
    except Exception as e:  # noqa: BLE001
        context.log.error(f"Error querying Jira issues: {e}")
        return dg.SkipReason(f"Failed to query Jira: {e}")

    issues = search_res.get("issues", [])
    if not issues:
        return dg.SkipReason("No Jira issues found matching criteria.")

    run_requests: list[dg.RunRequest] = []
    newest_created_str = last_created_str

    for issue in issues:
        issue_key = issue.get("key")
        if not issue_key or issue_key in seen_keys_set:
            continue

        fields = issue.get("fields", {})
        summary = fields.get("summary", "")
        created = fields.get("created", "")
        issue_type = fields.get("issuetype", {}).get("name", "")
        status = fields.get("status", {}).get("name", "")

        context.log.info(
            f"Found new Jira issue {issue_key}: '{summary}' (created: {created})"
        )

        run_requests.append(
            dg.RunRequest(
                run_key=f"jira_test_plan_{issue_key}",
                run_config={"ops": {"jira_issue": {"config": {"jira_id": issue_key}}}},
                tags={
                    "jira_id": issue_key,
                    "issue_key": issue_key,
                    "summary": summary,
                    "issue_type": issue_type,
                    "status": status,
                },
            )
        )

        seen_keys_set.add(issue_key)
        if created:
            if not newest_created_str:
                newest_created_str = created
            else:
                curr_dt = _parse_jira_datetime(created)
                newest_dt = _parse_jira_datetime(newest_created_str)
                if curr_dt and newest_dt and curr_dt > newest_dt:
                    newest_created_str = created

    updated_seen_keys = list(seen_keys_set)[-200:]
    new_cursor = json.dumps(
        {
            "last_created": newest_created_str,
            "seen_keys": updated_seen_keys,
        }
    )
    context.update_cursor(new_cursor)

    if not run_requests:
        return dg.SkipReason("No new unseen Jira issues found.")

    context.log.info(
        f"Generated {len(run_requests)} RunRequest(s) for test plan and test cases pipeline."
    )
    return run_requests
