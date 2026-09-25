import json
import os
import re
from pathlib import Path
from typing import Any

import dagster as dg
from backend.defs.resources import LLMClient


def _clean_and_parse_json(raw_text: str) -> Any:
    """Cleans and parses a raw JSON string from LLM output, handling markdown fences and extraneous text."""
    cleaned = raw_text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
        cleaned = cleaned.strip()

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        start_bracket = cleaned.find("[")
        end_bracket = cleaned.rfind("]")
        if start_bracket != -1 and end_bracket != -1 and end_bracket > start_bracket:
            sub = cleaned[start_bracket : end_bracket + 1]
            try:
                return json.loads(sub)
            except json.JSONDecodeError:
                pass

        start_brace = cleaned.find("{")
        end_brace = cleaned.rfind("}")
        if start_brace != -1 and end_brace != -1 and end_brace > start_brace:
            sub = cleaned[start_brace : end_brace + 1]
            try:
                return json.loads(sub)
            except json.JSONDecodeError:
                pass
        raise


def _normalize_test_cases(
    raw_data: Any, jira_id: str, jira_link: str
) -> list[dict]:
    """Normalizes parsed test case data into a uniform list of test case dictionaries."""
    items: list[dict] = []
    if isinstance(raw_data, list):
        items = [x for x in raw_data if isinstance(x, dict)]
    elif isinstance(raw_data, dict):
        if "test_cases" in raw_data and isinstance(raw_data["test_cases"], list):
            items = [x for x in raw_data["test_cases"] if isinstance(x, dict)]
        elif "tests" in raw_data and isinstance(raw_data["tests"], list):
            items = [x for x in raw_data["tests"] if isinstance(x, dict)]
        else:
            items = [raw_data]

    normalized: list[dict] = []
    for idx, item in enumerate(items, start=1):
        case_jira_id = str(item.get("jira_id") or jira_id)
        case_jira_link = str(
            item.get("jira_link")
            or item.get("jira_url")
            or item.get("link")
            or jira_link
        )

        title = str(item.get("title") or item.get("name") or f"Test Case {idx}").strip()
        description = str(
            item.get("description") or item.get("detailed_description") or ""
        ).strip()

        # Labels (nice to have)
        raw_labels = item.get("labels") or item.get("label") or []
        if isinstance(raw_labels, str):
            labels = [lbl.strip() for lbl in raw_labels.split(",") if lbl.strip()]
        elif isinstance(raw_labels, list):
            labels = [str(lbl).strip() for lbl in raw_labels if str(lbl).strip()]
        else:
            labels = [str(raw_labels).strip()] if str(raw_labels).strip() else []

        # Preconditions (nice to have)
        raw_preconditions = (
            item.get("preconditions")
            or item.get("precondition")
            or item.get("prerequisites")
            or []
        )
        if isinstance(raw_preconditions, str):
            preconditions = [
                p.strip() for p in raw_preconditions.splitlines() if p.strip()
            ]
        elif isinstance(raw_preconditions, list):
            preconditions = [
                str(p).strip() for p in raw_preconditions if str(p).strip()
            ]
        else:
            preconditions = (
                [str(raw_preconditions).strip()]
                if str(raw_preconditions).strip()
                else []
            )

        # Steps with actions and expected results
        raw_steps = item.get("steps") or []
        steps: list[dict] = []
        if isinstance(raw_steps, list):
            for s_idx, step in enumerate(raw_steps, start=1):
                if isinstance(step, dict):
                    raw_num = (
                        step.get("step_number")
                        or step.get("number")
                        or step.get("id")
                    )
                    if raw_num is None and isinstance(step.get("step"), (int, float)):
                        raw_num = step.get("step")
                    elif raw_num is None and isinstance(step.get("step"), str) and step.get("step").isdigit():
                        raw_num = int(step.get("step"))

                    try:
                        num = int(raw_num) if raw_num is not None else s_idx
                    except (ValueError, TypeError):
                        num = s_idx

                    action = (
                        step.get("action")
                        or step.get("description")
                        or step.get("name")
                        or step.get("step_text")
                        or (
                            step.get("step")
                            if isinstance(step.get("step"), str)
                            and not step.get("step").isdigit()
                            else ""
                        )
                        or ""
                    )
                    expected = (
                        step.get("expected_result")
                        or step.get("expected_results")
                        or step.get("expected")
                        or step.get("result")
                        or ""
                    )
                    steps.append(
                        {
                            "step_number": num,
                            "action": str(action).strip(),
                            "expected_result": str(expected).strip(),
                        }
                    )
                elif isinstance(step, str):
                    steps.append(
                        {
                            "step_number": s_idx,
                            "action": step.strip(),
                            "expected_result": "",
                        }
                    )
        elif isinstance(raw_steps, str):
            for s_idx, line in enumerate(raw_steps.splitlines(), start=1):
                if line.strip():
                    steps.append(
                        {
                            "step_number": s_idx,
                            "action": line.strip(),
                            "expected_result": "",
                        }
                    )

        normalized.append(
            {
                "jira_id": case_jira_id,
                "jira_link": case_jira_link,
                "title": title,
                "description": description,
                "labels": labels,
                "preconditions": preconditions,
                "steps": steps,
            }
        )

    return normalized


@dg.asset
async def test_plan(
    context: dg.AssetExecutionContext,
    processed_jira_issue: dict,
    epic_requirements: list[dict],
    llm_client: LLMClient,
):
    jira_id = processed_jira_issue.get("jira_id", "N/A")
    title = processed_jira_issue.get("title", "")
    description = (
        processed_jira_issue.get("description", "").strip()
        or "No description provided."
    )

    context_blocks = []

    # 1. Load documents passed from upstream epic_requirements asset
    if epic_requirements and isinstance(epic_requirements, list):
        for doc in epic_requirements:
            doc_title = doc.get("title", "Untitled Document")
            page_id = doc.get("page_id", "")
            url = doc.get("url", "")
            content = doc.get("content", "").strip()
            if content:
                header = f"#### Confluence Doc: {doc_title}"
                if page_id:
                    header += f" (Page ID: {page_id})"
                meta = f"**URL:** {url}\n\n" if url else ""
                context_blocks.append(f"{header}\n{meta}{content}")

    # 2. Fallback / supplement from data/{safe_jira_id} or data/context/{safe_jira_id} if available
    safe_jira_id = (
        re.sub(r"[^\w\-]", "_", jira_id).strip("_")
        if jira_id and jira_id != "N/A"
        else "test_plan"
    )
    data_dir = Path("data")
    if not data_dir.exists() and Path("backend/data").exists():
        data_dir = Path("backend/data")

    candidate_dirs = [
        data_dir / "context" / safe_jira_id,
        data_dir / safe_jira_id,
        Path("data") / "context" / safe_jira_id,
        Path("data") / safe_jira_id,
        Path("backend/data") / "context" / safe_jira_id,
        Path("backend/data") / safe_jira_id,
    ]

    if not context_blocks:
        seen_dirs = set()
        for c_dir in candidate_dirs:
            try:
                res_dir = c_dir.resolve()
            except OSError:
                continue
            if res_dir in seen_dirs:
                continue
            seen_dirs.add(res_dir)
            if c_dir.exists() and c_dir.is_dir():
                for md_file in sorted(c_dir.glob("*.md")):
                    if (
                        md_file.name == "test_plan.md"
                        or md_file.name.endswith("_test_plan.md")
                    ):
                        continue
                    try:
                        file_text = md_file.read_text(encoding="utf-8").strip()
                        if file_text:
                            context_blocks.append(file_text)
                    except OSError as e:
                        context.log.warning(
                            f"Failed to read context file {md_file}: {e}"
                        )
                if context_blocks:
                    break

    if context_blocks:
        context_text = "\n\n---\n\n".join(context_blocks)
        context_section = f"""### Linked Confluence Requirements & System Context
The following {len(context_blocks)} Confluence document(s) were retrieved as technical, architectural, and business context for this epic / feature:

{context_text}"""
    else:
        context_section = "### Linked Confluence Requirements & System Context\nNo linked Confluence context documents were found."

    prompt = f"""You are a Principal QA Architect and Lead Test Strategist.
Create a comprehensive, production-grade Testing Plan of Action (Test Strategy & Architecture Blueprint) in Markdown format for the following Jira issue.

Thoroughly analyze and synthesize the Jira issue details along with all provided Confluence requirements, technical specifications, acceptance criteria, data models, and architectural context documents. Ground all testing strategies, acceptance criteria breakdown, boundary checks, and risk analyses directly in these specifications.

### Downstream Purpose
This document is the high-level Testing Plan of Action that will be ingested by a subsequent automated step to generate individual test cases and executable test suites.

IMPORTANT CONSTRAINT:
Do NOT generate individual test cases, step-by-step test procedures, or tabular test case inventories in this step. Focus entirely on the testing plan of action, test strategy, scope, architectural requirements, environment & data prerequisites, risk mitigations, and directives for downstream test case synthesis.

### Jira Issue Details
- **Issue Key:** {jira_id}
- **Title:** {title}
- **Description:**
{description}

{context_section}

### Testing Plan of Action Structure & Requirements
Generate a structured Markdown document covering the following sections:

# Test Plan of Action: [{jira_id}] {title}

## 1. Technical Overview & Requirements Decomposition
- **Feature & Architecture Summary:** Technical summary of what is being built or modified, and how it fits into the broader system.
- **Acceptance Criteria (AC) Breakdown:** Discrete, testable functional and non-functional requirements extracted from the Jira issue and Confluence specs.
- **Core Quality Objectives:** High-level testing goals and quality targets (e.g., zero data leakage, $O(1)$ memory streaming, strict schema conformance, semantic accuracy).

## 2. Scope & Testing Boundaries
- **In-Scope Areas:** Specific modules, services, database models, API endpoints, and critical business flows targeted for testing.
- **Out-of-Scope Areas:** Explicitly unimpacted services, upstream pipelines, or external dependencies excluded from this testing cycle.
- **Assumptions & Upstream Dependencies:** Key technical prerequisites and system invariants assumed to be true.

## 3. Test Strategy & Plan of Action by Testing Layer
Elaborate on the strategic plan of action for each relevant level of the test pyramid:
- **Unit Testing Strategy:** Logic to test in isolation, algorithms, state machines, validation rules, and mocking boundaries.
- **Integration & Persistence Testing Strategy:** Database interactions, transactional integrity, streaming cursors, multi-tenant schemas, caching layers, and external service contracts.
- **API & Contract Testing Strategy:** Endpoint verification, request/response schema adherence, header handling (authentication and tenant context), and status code behaviors.
- **Security & Multi-Tenancy Strategy:** Data isolation enforcement across tenant boundaries, token validity, permission checks, and injection protection.
- **Resilience, Reliability & Negative Testing Strategy:** Error handling protocols, graceful degradation, upstream/downstream timeout handling, and partial failure recovery.
- **Performance & Resource Utilization Strategy:** Latency expectations, memory profiling, prevention of $N+1$ query patterns, and resource footprint boundaries.

## 4. Test Environment, Tooling & Test Data Strategy
- **Environment & Infrastructure Requirements:** Required runtimes, databases (e.g., PostgreSQL with specific extensions), containers, and configuration toggles.
- **Test Frameworks & Tooling Stack:** Recommended automation tools, runners, and diagnostic profilers.
- **Test Data & Fixture Architecture:** Strategy for test datasets (seed data requirements, tenant profiles, stale/active states, and fixture teardown).
- **Mocking & Service Virtualization Strategy:** Guidelines on what external dependencies to mock versus run live.

## 5. Risk Assessment, Failure Modes & Regression Strategy
- **Identified Technical Risks & Failure Modes:** High-risk architectural areas, edge conditions, and failure scenarios.
- **Regression Impact Analysis:** Existing components and downstream features potentially impacted by these changes.
- **Risk Mitigation Tactics:** Specific testing safeguards designed to mitigate each identified risk.

## 6. Directives for Downstream Test Case Generation
Provide explicit guidelines for the downstream agent that will generate concrete test cases from this plan:
- **Critical Path & Priority Mapping:** Key scenarios that must be covered under P0 (Blocker), P1 (High), and P2 (Medium).
- **Target Categories to Synthesize:** Positive flows, negative/error paths, boundary limits, and security tests.
- **Required Assertion Standards:** Specific variables, status codes, exception types, and state transitions to verify.

## 7. Definition of Done & Quality Gates
- **Entry Criteria:** Prerequisites before test execution can commence (e.g., environment readiness, baseline schema migrations).
- **Exit Criteria / Quality Gates:** Target pass rates (e.g., 100% P0/P1), coverage requirements, zero open critical defects, and sign-off criteria.

### Formatting & Quality Rules
- Write clean, professional Markdown with rigorous technical depth.
- Maintain strategic focus: explain *what* to test, *why*, and *how to approach it*, leaving the generation of individual test cases/steps to the downstream phase.
"""

    response = await llm_client.generate(prompt)
    test_plan_content = response.text or ""

    # Ensure /data/{jira_id} directory exists
    issue_dir = data_dir / safe_jira_id
    issue_dir.mkdir(parents=True, exist_ok=True)

    file_path = issue_dir / "test_plan.md"
    with file_path.open("w", encoding="utf-8") as f:
        f.write(test_plan_content)

    return dg.MaterializeResult(
        value=test_plan_content,
        metadata={
            "file_path": dg.MetadataValue.path(str(file_path.resolve())),
            "jira_id": jira_id,
            "context_docs_count": len(context_blocks),
            "preview": dg.MetadataValue.md(test_plan_content),
        },
    )


@dg.asset
async def test_cases(
    context: dg.AssetExecutionContext,
    processed_jira_issue: dict,
    test_plan: str,
    llm_client: LLMClient,
):
    jira_id = processed_jira_issue.get("jira_id", "N/A")
    title = processed_jira_issue.get("title", "")
    description = (
        processed_jira_issue.get("description", "").strip()
        or "No description provided."
    )
    jira_link = processed_jira_issue.get("url") or ""
    if not jira_link and jira_id and jira_id != "N/A":
        base_url = os.getenv("BASE_URL", "").rstrip("/")
        if base_url:
            jira_link = f"{base_url}/browse/{jira_id}"
        else:
            jira_link = f"/browse/{jira_id}"

    safe_jira_id = (
        re.sub(r"[^\w\-]", "_", jira_id).strip("_")
        if jira_id and jira_id != "N/A"
        else "test_cases"
    )

    test_plan_text = ""
    if isinstance(test_plan, str):
        test_plan_text = test_plan
    elif hasattr(test_plan, "value"):
        test_plan_text = str(test_plan.value or "")

    # Fallback to reading data/{safe_jira_id}/test_plan.md if available
    if not test_plan_text:
        candidates = [
            Path("data") / safe_jira_id / "test_plan.md",
            Path("backend/data") / safe_jira_id / "test_plan.md",
            Path("data") / f"{safe_jira_id}_test_plan.md",
            Path("backend/data") / f"{safe_jira_id}_test_plan.md",
        ]
        for plan_file in candidates:
            if plan_file.exists():
                try:
                    test_plan_text = plan_file.read_text(encoding="utf-8").strip()
                    if test_plan_text:
                        break
                except OSError as e:
                    context.log.warning(
                        f"Failed to read test plan file {plan_file}: {e}"
                    )

    prompt = f"""You are a Principal QA Automation Architect and Lead Test Engineer.
Based on the provided Testing Plan of Action and Jira issue, generate a comprehensive suite of detailed, actionable test cases formatted as a strict JSON array.

### Jira Issue Information
- **Issue Key:** {jira_id}
- **Issue Link:** {jira_link}
- **Title:** {title}
- **Description:**
{description}

### Test Plan of Action
{test_plan_text}

### Instructions & Coverage Objectives
1. Deconstruct the Test Plan of Action (Acceptance Criteria, Test Strategy layers: Unit, Integration & Persistence, API & Contract, Security & Multi-Tenancy, Resilience & Reliability, Performance, and Directives for Downstream Generation).
2. Synthesize concrete, executable test cases that rigorously address all identified risk areas and quality gates.
3. Include critical path positive (happy path) flows, edge cases, boundary limits, negative/error validation, security/isolation verifications, and resilience/recovery scenarios across P0, P1, and P2 priority tiers.
4. Each test case MUST be exhaustive and standalone, providing explicit preconditions, actions, and exact assertions/expected results.

### Test Case JSON Schema Requirements
Return a JSON array containing objects with the following schema:
- `jira_id` (string): The Jira issue key (e.g. "{jira_id}").
- `jira_link` (string): Link to the original Jira issue (e.g. "{jira_link}").
- `title` (string): Specific, clear title describing the exact condition and expected behavior.
- `description` (string): Detailed description outlining the scope, test objective, testing layer, and business/technical rationale.
- `labels` (array of strings): Applicable tags/labels, including priority ("P0", "P1", "P2"), layer ("Unit", "Integration", "API", "Security", "Resilience", "Performance"), test type ("Positive", "Negative", "Boundary", "Regression"), and module tags.
- `preconditions` (array of strings): Specific environment state, database fixtures, mocked services, tokens, or configuration required before executing the test.
- `steps` (array of objects): Step-by-step procedure. Each step MUST contain:
  - `step_number` (integer): Sequential step index starting at 1.
  - `action` (string): Precise action performed (e.g. API payload sent, function called, DB state mutated).
  - `expected_result` (string): Concrete, measurable outcome (expected status code, response body fields, DB state, log message, error code).

### JSON Output Format Example
```json
[
  {{
    "jira_id": "{jira_id}",
    "jira_link": "{jira_link}",
    "title": "Verify API endpoint rejects unauthenticated request with 401 Unauthorized",
    "description": "Assert that requests lacking a valid Bearer token are rejected immediately prior to business logic execution.",
    "labels": ["P0", "Security", "API", "Negative"],
    "preconditions": [
      "API service is running and configured with token authentication middleware",
      "No Authorization header is attached to the request"
    ],
    "steps": [
      {{
        "step_number": 1,
        "action": "Send POST request to /api/v1/analyze without Authorization header and valid JSON payload.",
        "expected_result": "HTTP 401 Unauthorized is returned."
      }},
      {{
        "step_number": 2,
        "action": "Validate the error response body schema.",
        "expected_result": "Response contains error code 'UNAUTHORIZED' and does not leak stack trace or internal details."
      }}
    ]
  }}
]
```

IMPORTANT:
Return ONLY the valid JSON array of test cases. Do not include extraneous narrative text before or after the JSON.
"""

    try:
        response = await llm_client.generate(
            prompt,
            config={"response_mime_type": "application/json"},
        )
    except Exception as e:  # noqa: BLE001
        context.log.warning(
            f"Failed to generate test cases with application/json mime type ({e}), retrying without config..."
        )
        response = await llm_client.generate(prompt)

    raw_response_text = response.text or ""

    try:
        raw_json = _clean_and_parse_json(raw_response_text)
    except Exception as e:  # noqa: BLE001
        context.log.error(
            f"Failed to parse LLM JSON output: {e}. Raw text: {raw_response_text[:500]}"
        )
        raw_json = []

    test_cases_list = _normalize_test_cases(
        raw_json, jira_id=jira_id, jira_link=jira_link
    )

    # Store these as a json file under /data/{jira_id}/tests.json
    data_dir = Path("data")
    if not data_dir.exists() and Path("backend/data").exists():
        data_dir = Path("backend/data")
    issue_dir = data_dir / safe_jira_id
    issue_dir.mkdir(parents=True, exist_ok=True)

    file_path = issue_dir / "tests.json"
    with file_path.open("w", encoding="utf-8") as f:
        json.dump(test_cases_list, f, indent=2, ensure_ascii=False)

    return dg.MaterializeResult(
        value=test_cases_list,
        metadata={
            "file_path": dg.MetadataValue.path(str(file_path.resolve())),
            "jira_id": jira_id,
            "test_case_count": len(test_cases_list),
            "preview": dg.MetadataValue.json(test_cases_list),
        },
    )


# Prevent pytest from treating Dagster asset definitions as test functions
test_plan.__test__ = False
test_cases.__test__ = False
