import json
import os
import shutil
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import dagster as dg
import pytest
from backend.defs.assets.jira import (
    JiraIssue,
    epic_requirements,
    load_existing_confluence_context,
    processed_jira_issue,
)
from backend.defs.assets.jira import (
    jira_issue as jira_issue_asset,
)
from backend.defs.assets.jira import (
    jira_issue_epic as jira_issue_epic_asset,
)
from backend.defs.assets.test_plan import (
    _clean_and_parse_json,
    _normalize_test_cases,
)
from backend.defs.assets.test_plan import (
    test_cases as cases_asset,
)
from backend.defs.assets.test_plan import (
    test_plan as plan_asset,
)


def test_clean_and_parse_json_plain():
    data = [{"title": "Test 1"}]
    raw = json.dumps(data)
    assert _clean_and_parse_json(raw) == data


def test_clean_and_parse_json_markdown_fence():
    data = [
        {
            "title": "Test Fence",
            "steps": [{"step_number": 1, "action": "Act", "expected_result": "Exp"}],
        }
    ]
    raw = f"```json\n{json.dumps(data, indent=2)}\n```"
    assert _clean_and_parse_json(raw) == data


def test_clean_and_parse_json_surrounding_text():
    data = [{"title": "Test Surrounding"}]
    raw = f"Here is the generated test suite:\n```json\n{json.dumps(data)}\n```\nHope this helps!"
    assert _clean_and_parse_json(raw) == data


def test_clean_and_parse_json_dict_wrapper():
    data = {"test_cases": [{"title": "Test in Dict"}]}
    raw = json.dumps(data)
    assert _clean_and_parse_json(raw) == data


def test_normalize_test_cases_complete():
    raw = [
        {
            "jira_id": "SIS-1550",
            "jira_link": "https://example.atlassian.net/browse/SIS-1550",
            "title": "Verify AST parser handles malformed input",
            "description": "Ensure the parser catches syntax errors without crashing.",
            "labels": ["P0", "Unit", "Parser"],
            "preconditions": ["Parser engine initialized"],
            "steps": [
                {
                    "step_number": 1,
                    "action": "Send invalid syntax",
                    "expected_result": "Error flag raised",
                }
            ],
        }
    ]
    normalized = _normalize_test_cases(
        raw,
        jira_id="SIS-1550",
        jira_link="https://example.atlassian.net/browse/SIS-1550",
    )
    assert len(normalized) == 1
    tc = normalized[0]
    assert tc["jira_id"] == "SIS-1550"
    assert tc["jira_link"] == "https://example.atlassian.net/browse/SIS-1550"
    assert tc["title"] == "Verify AST parser handles malformed input"
    assert (
        tc["description"] == "Ensure the parser catches syntax errors without crashing."
    )
    assert tc["labels"] == ["P0", "Unit", "Parser"]
    assert tc["preconditions"] == ["Parser engine initialized"]
    assert len(tc["steps"]) == 1
    assert tc["steps"][0] == {
        "step_number": 1,
        "action": "Send invalid syntax",
        "expected_result": "Error flag raised",
    }


def test_normalize_test_cases_fallbacks_and_string_conversions():
    raw = {
        "test_cases": [
            {
                "name": "Fallback Test Case",
                "detailed_description": "A description from alias key",
                "label": "P1, Integration, Security",
                "precondition": "Service running\nToken active",
                "steps": [
                    {"step": 1, "description": "Call endpoint", "expected": "200 OK"},
                    "Raw step string without dict",
                ],
            }
        ]
    }
    normalized = _normalize_test_cases(
        raw, jira_id="SIS-999", jira_link="https://company.atlassian.net/browse/SIS-999"
    )
    assert len(normalized) == 1
    tc = normalized[0]
    assert tc["jira_id"] == "SIS-999"
    assert tc["jira_link"] == "https://company.atlassian.net/browse/SIS-999"
    assert tc["title"] == "Fallback Test Case"
    assert tc["description"] == "A description from alias key"
    assert tc["labels"] == ["P1", "Integration", "Security"]
    assert tc["preconditions"] == ["Service running", "Token active"]
    assert len(tc["steps"]) == 2
    assert tc["steps"][0] == {
        "step_number": 1,
        "action": "Call endpoint",
        "expected_result": "200 OK",
    }
    assert tc["steps"][1] == {
        "step_number": 2,
        "action": "Raw step string without dict",
        "expected_result": "",
    }


def test_processed_jira_issue_url_resolution(monkeypatch):
    monkeypatch.setenv("BASE_URL", "https://custom.atlassian.net")
    issue_data = {
        "key": "SIS-1234",
        "fields": {
            "summary": "Sample Summary",
            "description": None,
        },
        "self": "https://custom.atlassian.net/rest/api/3/issue/10002",
    }
    result = processed_jira_issue(issue_data)
    assert result["jira_id"] == "SIS-1234"
    assert result["title"] == "Sample Summary"
    assert result["url"] == "https://custom.atlassian.net/browse/SIS-1234"


def test_processed_jira_issue_fallback_without_base_url(monkeypatch):
    monkeypatch.delenv("BASE_URL", raising=False)
    issue_data = {
        "key": "SIS-5678",
        "fields": {"summary": "No Base URL", "description": None},
    }
    result = processed_jira_issue(issue_data)
    assert result["jira_id"] == "SIS-5678"
    assert result["url"] == "/browse/SIS-5678"


def test_jira_issue_asset_sanitizes_metadata():
    mock_jira_client = MagicMock()
    mock_jira_client.get.return_value.json.return_value = {
        "key": "SIS-100",
        "fields": {
            "summary": "Implement secure auth",
            "status": {"name": "In Progress"},
            "issuetype": {"name": "Story"},
            "reporter": {
                "emailAddress": "secret@company.internal",
                "accountId": "acc-12345",
            },
        },
    }

    config = JiraIssue(jira_id="SIS-100")
    result = jira_issue_asset(config=config, jira_client=mock_jira_client)

    assert isinstance(result, dg.MaterializeResult)
    assert result.metadata == {
        "jira_id": "SIS-100",
        "summary": "Implement secure auth",
        "status": "In Progress",
        "issue_type": "Story",
    }
    assert "reporter" not in result.metadata


def test_jira_issue_epic_asset_sanitizes_metadata():
    mock_jira_client = MagicMock()
    mock_jira_client.get.return_value.json.return_value = {
        "key": "SIS-50",
        "fields": {
            "summary": "Epic summary",
            "status": {"name": "To Do"},
            "assignee": {"emailAddress": "admin@company.internal"},
        },
    }

    context = dg.build_asset_context()
    jira_issue = {
        "fields": {
            "parent": {
                "fields": {"issuetype": {"name": "Epic"}},
                "self": "https://custom.atlassian.net/rest/api/3/issue/10050",
            }
        }
    }

    result = jira_issue_epic_asset(
        context=context,
        jira_issue=jira_issue,
        jira_client=mock_jira_client,
    )

    assert isinstance(result, dg.MaterializeResult)
    assert result.metadata == {
        "jira_id": "SIS-50",
        "summary": "Epic summary",
        "status": "To Do",
    }
    assert "assignee" not in result.metadata


@pytest.mark.anyio
async def test_test_cases_asset_materialization(tmp_path, monkeypatch):
    """Test materializing test_cases asset and verifying tests.json creation."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BASE_URL", "https://example.atlassian.net")

    mock_llm_client = MagicMock()
    fake_response = MagicMock()
    sample_cases = [
        {
            "jira_id": "SIS-1550",
            "jira_link": "https://example.atlassian.net/browse/SIS-1550",
            "title": "Verify Direct Impact Classification",
            "description": "Ensure modifying a function marks downstream callers as Indirect impact.",
            "labels": ["P0", "Integration"],
            "preconditions": ["Impact graph database loaded with test AST nodes"],
            "steps": [
                {
                    "step_number": 1,
                    "action": "Submit code diff modifying core parser method.",
                    "expected_result": "Impact analysis returns Direct on parser method and Indirect on consumers.",
                }
            ],
        }
    ]
    fake_response.text = json.dumps(sample_cases)
    mock_llm_client.generate = AsyncMock(return_value=fake_response)

    context = dg.build_asset_context(resources={"llm_client": mock_llm_client})

    processed_issue = {
        "jira_id": "SIS-1550",
        "title": "Change Impact Analyser",
        "description": "Determines downstream blast radius of code changes.",
        "url": "https://example.atlassian.net/browse/SIS-1550",
    }
    test_plan_content = (
        "# Test Plan: [SIS-1550] Change Impact Analyser\n## 1. Technical Overview..."
    )

    result = await cases_asset(
        context=context,
        processed_jira_issue=processed_issue,
        test_plan=test_plan_content,
    )

    assert isinstance(result, dg.MaterializeResult)
    assert len(result.value) == 1
    assert result.value[0]["title"] == "Verify Direct Impact Classification"

    # Verify /data/{jira_id}/tests.json was written
    target_json_path = tmp_path / "data" / "SIS-1550" / "tests.json"
    assert target_json_path.exists()

    saved_data = json.loads(target_json_path.read_text(encoding="utf-8"))
    assert isinstance(saved_data, list)
    assert len(saved_data) == 1
    saved_case = saved_data[0]
    assert saved_case["jira_id"] == "SIS-1550"
    assert saved_case["jira_link"] == "https://example.atlassian.net/browse/SIS-1550"
    assert saved_case["title"] == "Verify Direct Impact Classification"
    assert (
        saved_case["description"]
        == "Ensure modifying a function marks downstream callers as Indirect impact."
    )
    assert saved_case["labels"] == ["P0", "Integration"]
    assert saved_case["preconditions"] == [
        "Impact graph database loaded with test AST nodes"
    ]
    assert len(saved_case["steps"]) == 1
    assert saved_case["steps"][0]["step_number"] == 1
    assert (
        saved_case["steps"][0]["action"]
        == "Submit code diff modifying core parser method."
    )
    assert (
        saved_case["steps"][0]["expected_result"]
        == "Impact analysis returns Direct on parser method and Indirect on consumers."
    )


@pytest.mark.anyio
async def test_test_plan_asset_materialization(tmp_path, monkeypatch):
    """Test materializing test_plan asset and verifying test_plan.md creation under /data/{jira_id}/test_plan.md."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BASE_URL", "https://example.atlassian.net")

    mock_llm_client = MagicMock()
    fake_response = MagicMock()
    fake_response.text = "# Test Plan of Action: [SIS-1550] Change Impact Analyser\n\n## 1. Technical Overview..."
    mock_llm_client.generate = AsyncMock(return_value=fake_response)

    context = dg.build_asset_context(resources={"llm_client": mock_llm_client})

    processed_issue = {
        "jira_id": "SIS-1550",
        "title": "Change Impact Analyser",
        "description": "Determines downstream blast radius.",
        "url": "https://example.atlassian.net/browse/SIS-1550",
    }
    epic_reqs = [
        {
            "page_id": "9672818783",
            "title": "Change Impact Analyser - Business Spec",
            "url": "https://example.atlassian.net/wiki/pages/9672818783",
            "content": "Business requirements content",
        }
    ]

    result = await plan_asset(
        context=context,
        processed_jira_issue=processed_issue,
        epic_requirements=epic_reqs,
    )

    assert isinstance(result, dg.MaterializeResult)
    assert "# Test Plan of Action: [SIS-1550]" in result.value

    # Verify /data/{jira_id}/test_plan.md was created
    target_plan_path = tmp_path / "data" / "SIS-1550" / "test_plan.md"
    assert target_plan_path.exists()
    assert target_plan_path.read_text(encoding="utf-8") == fake_response.text


@pytest.mark.anyio
async def test_test_cases_asset_fallback_to_plan_file(tmp_path, monkeypatch):
    """Test test_cases fallback to data/{jira_id}/test_plan.md when test_plan string is empty."""
    monkeypatch.chdir(tmp_path)
    data_dir = tmp_path / "data" / "SIS-1550"
    data_dir.mkdir(parents=True, exist_ok=True)
    plan_file = data_dir / "test_plan.md"
    plan_file.write_text("# On-Disk Test Plan: [SIS-1550]", encoding="utf-8")

    mock_llm_client = MagicMock()
    fake_response = MagicMock()
    sample_cases = [
        {
            "title": "On disk plan test",
            "description": "Verified on-disk plan ingestion",
            "steps": [
                {"step_number": 1, "action": "Run step", "expected_result": "Success"}
            ],
        }
    ]
    fake_response.text = json.dumps(sample_cases)
    mock_llm_client.generate = AsyncMock(return_value=fake_response)

    context = dg.build_asset_context(resources={"llm_client": mock_llm_client})

    processed_issue = {
        "jira_id": "SIS-1550",
        "title": "Change Impact Analyser",
        "description": "Determines downstream blast radius of code changes.",
    }

    result = await cases_asset(
        context=context,
        processed_jira_issue=processed_issue,
        test_plan="",
    )

    assert isinstance(result, dg.MaterializeResult)
    assert len(result.value) == 1
    assert result.value[0]["title"] == "On disk plan test"
    assert result.value[0]["jira_id"] == "SIS-1550"
    assert "SIS-1550" in result.value[0]["jira_link"]


@pytest.mark.anyio
async def test_test_cases_asset_invalid_json_handling(tmp_path, monkeypatch):
    """Test test_cases asset handles malformed LLM response without crashing."""
    monkeypatch.chdir(tmp_path)

    mock_llm_client = MagicMock()
    fake_response = MagicMock()
    fake_response.text = "This is not valid json at all and cannot be parsed."
    mock_llm_client.generate = AsyncMock(return_value=fake_response)

    context = dg.build_asset_context(resources={"llm_client": mock_llm_client})

    processed_issue = {
        "jira_id": "SIS-1550",
        "title": "Change Impact Analyser",
    }

    result = await cases_asset(
        context=context,
        processed_jira_issue=processed_issue,
        test_plan="Some plan",
    )

    assert isinstance(result, dg.MaterializeResult)
    assert result.value == []
    target_json_path = tmp_path / "data" / "SIS-1550" / "tests.json"
    assert target_json_path.exists()
    assert json.loads(target_json_path.read_text(encoding="utf-8")) == []


def test_epic_requirements_reuses_existing_context(tmp_path, monkeypatch):
    """Verify that if context for an epic already exists, epic_requirements returns cached values without re-scraping."""
    monkeypatch.chdir(tmp_path)

    # Set up existing context directory with markdown file
    context_dir = tmp_path / "data" / "context" / "SIS-1074"
    context_dir.mkdir(parents=True, exist_ok=True)
    doc_path = context_dir / "1828159522_Test_Cases_Maintainer.md"
    doc_content = (
        "# Test Cases Maintainer\n\n"
        "- **Page ID:** 1828159522\n"
        "- **Confluence URL:** https://example.atlassian.net/wiki/pages/1828159522\n\n"
        "---\n\n"
        "Existing architectural details for Test Cases Maintainer."
    )
    doc_path.write_text(doc_content, encoding="utf-8")

    mock_jira_client = MagicMock()
    context = dg.build_asset_context(resources={"jira_client": mock_jira_client})

    jira_issue_epic = {
        "key": "SIS-1074",
        "fields": {"summary": "Test Cases Maintainer Epic"},
    }

    result = epic_requirements(
        context=context,
        jira_issue_epic=jira_issue_epic,
    )

    assert isinstance(result, dg.MaterializeResult)
    assert result.metadata["cached"] is True
    assert result.metadata["count"] == 1
    assert result.metadata["jira_id"] == "SIS-1074"
    assert len(result.value) == 1
    assert result.value[0]["page_id"] == "1828159522"
    assert result.value[0]["title"] == "Test Cases Maintainer"
    assert "Existing architectural details" in result.value[0]["content"]

    # Verify Jira client was NEVER called to fetch remotelinks or pages
    mock_jira_client.get.assert_not_called()


def test_load_existing_confluence_context_handles_nonexistent(tmp_path, monkeypatch):
    """Verify load_existing_confluence_context returns empty list when no directory exists."""
    monkeypatch.chdir(tmp_path)
    res = load_existing_confluence_context("NONEXISTENT-999")
    assert res == []


cases_asset.__test__ = False
plan_asset.__test__ = False
epic_requirements.__test__ = False
jira_issue_asset.__test__ = False
jira_issue_epic_asset.__test__ = False
