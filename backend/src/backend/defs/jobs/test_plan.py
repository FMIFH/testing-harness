import dagster as dg

from backend.defs.assets.jira import (
    epic_requirements,
    jira_issue,
    jira_issue_epic,
    processed_jira_issue,
)
from backend.defs.assets.test_plan import test_cases, test_plan

test_plan_job = dg.define_asset_job(
    name="test_plan_job",
    selection=dg.AssetSelection.assets(
        jira_issue,
        jira_issue_epic,
        epic_requirements,
        processed_jira_issue,
        test_plan,
        test_cases,
    ),
    description="Pipeline job that fetches a Jira issue, extracts Epic Confluence requirements, and generates the Test Plan of Action and detailed Test Cases.",
)
