import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import dagster as dg
import requests
from backend.defs.resources import JiraClient


class JiraIssue(dg.Config):
    jira_id: str


def _get_base_url(jira_client: JiraClient | None = None) -> str:
    """Safely retrieves the configured Jira/Atlassian Base URL without hardcoded fallback domains."""
    base_url = ""
    if jira_client and hasattr(jira_client, "base_url"):
        val = jira_client.base_url
        if isinstance(val, str) and val:
            base_url = val
        elif isinstance(val, dg.EnvVar):
            base_url = os.getenv(val.name, "")
    if not base_url:
        base_url = os.getenv("BASE_URL", "")
    return base_url.rstrip("/")


@dg.asset
def jira_issue(config: JiraIssue, jira_client: JiraClient):
    response = jira_client.get(f"rest/api/3/issue/{config.jira_id}")
    issue_data = response.json()
    fields = issue_data.get("fields", {}) if isinstance(issue_data, dict) else {}
    status_field = fields.get("status")
    status_name = (
        status_field.get("name", "") if isinstance(status_field, dict) else ""
    )
    issuetype_field = fields.get("issuetype")
    issuetype_name = (
        issuetype_field.get("name", "") if isinstance(issuetype_field, dict) else ""
    )
    return dg.MaterializeResult(
        value=issue_data,
        metadata={
            "jira_id": config.jira_id,
            "summary": fields.get("summary", ""),
            "status": status_name,
            "issue_type": issuetype_name,
        },
    )


def adf_to_markdown(
    node: dict | list | None, list_prefix: str = "", in_table: bool = False
) -> str:
    """Converts an Atlassian Document Format (ADF) structure into formatted Markdown."""
    if not node:
        return ""

    if isinstance(node, list):
        return "".join(
            adf_to_markdown(child, list_prefix, in_table=in_table) for child in node
        )

    if not isinstance(node, dict):
        return ""

    node_type = node.get("type")
    content = node.get("content", [])
    text = node.get("text", "")
    attrs = node.get("attrs", {})

    # Apply inline marks (e.g., bold, italic, code, strike, link)
    for mark in node.get("marks", []):
        mtype = mark.get("type")
        if mtype == "strong":
            text = f"**{text}**"
        elif mtype == "em":
            text = f"*{text}*"
        elif mtype == "code":
            text = f"`{text}`"
        elif mtype == "strike":
            text = f"~~{text}~~"
        elif mtype == "link":
            href = mark.get("attrs", {}).get("href", "")
            text = f"[{text}]({href})"

    if node_type == "doc":
        return adf_to_markdown(content, in_table=in_table).strip()
    elif node_type == "paragraph":
        inner = adf_to_markdown(content, in_table=in_table)
        val = f"{text}{inner}"
        return f"{val}<br>" if in_table else f"{val}\n\n"
    elif node_type == "heading":
        level = attrs.get("level", 1)
        inner = adf_to_markdown(content, in_table=in_table)
        return f"{'#' * level} {text}{inner}\n\n"
    elif node_type == "bulletList":
        res = "".join(
            adf_to_markdown(item, list_prefix="- ", in_table=in_table)
            for item in content
        )
        return res if in_table else f"{res}\n"
    elif node_type == "orderedList":
        res = "".join(
            adf_to_markdown(item, list_prefix=f"{i + 1}. ", in_table=in_table)
            for i, item in enumerate(content)
        )
        return res if in_table else f"{res}\n"
    elif node_type == "listItem":
        item_text = adf_to_markdown(content, in_table=in_table).strip()
        return f"{list_prefix}{item_text}\n"
    elif node_type == "taskList":
        res = "".join(adf_to_markdown(item, in_table=in_table) for item in content)
        return res if in_table else f"{res}\n"
    elif node_type == "taskItem":
        state = attrs.get("state", "TODO")
        box = "[x]" if state == "DONE" else "[ ]"
        item_text = adf_to_markdown(content, in_table=in_table).strip()
        return f"- {box} {item_text}\n"
    elif node_type == "codeBlock":
        lang = attrs.get("language", "")
        inner = adf_to_markdown(content, in_table=in_table)
        return f"```{lang}\n{text}{inner}\n```\n\n"
    elif node_type == "rule":
        return "---\n\n"
    elif node_type in ("inlineCard", "blockCard", "card"):
        url = attrs.get("url", "")
        return f"[{url}]({url})" if url else ""
    elif node_type == "mention":
        return attrs.get("text", "")
    elif node_type == "status":
        st_text = attrs.get("text", "")
        return f"**[{st_text}]**"
    elif node_type == "date":
        ts = attrs.get("timestamp")
        if ts:
            try:
                dt = datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc)
                return f"`{dt.strftime('%Y-%m-%d')}`"
            except (ValueError, TypeError, OSError):
                pass
        return ""
    elif node_type == "hardBreak":
        return "<br>" if in_table else "\n"
    elif node_type == "panel":
        ptype = attrs.get("panelType", "note").upper()
        inner = adf_to_markdown(content, in_table=in_table).strip()
        lines = [f"> {line}" for line in inner.splitlines()]
        return f"> **[{ptype}]**\n" + "\n".join(lines) + "\n\n"
    elif node_type == "blockquote":
        inner = adf_to_markdown(content, in_table=in_table).strip()
        lines = [f"> {line}" for line in inner.splitlines()]
        return "\n".join(lines) + "\n\n"
    elif node_type in ("expand", "nestedExpand"):
        exp_title = attrs.get("title", "Details")
        inner = adf_to_markdown(content, in_table=in_table).strip()
        return f"<details>\n<summary>{exp_title}</summary>\n\n{inner}\n</details>\n\n"
    elif node_type == "table":
        rows = []
        for row_node in content:
            if row_node.get("type") != "tableRow":
                continue
            cells = []
            for cell_node in row_node.get("content", []):
                cell_val = adf_to_markdown(
                    cell_node.get("content", []), in_table=True
                ).strip()
                cell_val = cell_val.replace("\n", " ").replace("|", "\\|")
                while cell_val.endswith("<br>"):
                    cell_val = cell_val[:-4].strip()
                while cell_val.startswith("<br>"):
                    cell_val = cell_val[4:].strip()
                cells.append(cell_val)
            rows.append(cells)
        if not rows:
            return ""
        max_cols = max(len(r) for r in rows)
        padded_rows = [r + [""] * (max_cols - len(r)) for r in rows]
        md_table = [
            "| " + " | ".join(padded_rows[0]) + " |",
            "| " + " | ".join(["---"] * max_cols) + " |",
        ]
        for r in padded_rows[1:]:
            md_table.append("| " + " | ".join(r) + " |")
        return "\n".join(md_table) + "\n\n"
    elif node_type in ("mediaSingle", "media"):
        alt = attrs.get("alt", "")
        return f"![{alt}]()" if alt else ""
    elif node_type == "emoji":
        return attrs.get("text") or attrs.get("shortName", "")

    inner = adf_to_markdown(content, list_prefix, in_table=in_table)
    return f"{text}{inner}"


def _extract_page_ids_from_text(text: str) -> set[str]:
    """Extracts Confluence page IDs from any URL string or raw text."""
    page_ids = set()
    if not text:
        return page_ids

    # Pattern 1: /pages/123456...
    for match in re.finditer(r"/pages/(\d+)", text):
        page_ids.add(match.group(1))

    # Pattern 2: pageId=123456...
    for match in re.finditer(r"pageId=(\d+)", text):
        page_ids.add(match.group(1))

    return page_ids


def fetch_confluence_page(
    jira_client: JiraClient, page_id: str, logger=None
) -> dict | None:
    """Fetches Confluence page details and content by page_id via REST API v2 with v1 fallback."""
    # Attempt Confluence REST API v2
    try:
        page_resp = jira_client.get(
            f"wiki/api/v2/pages/{page_id}?body-format=atlas_doc_format"
        )
        return page_resp.json()
    except (requests.RequestException, json.JSONDecodeError, KeyError) as e:
        if logger:
            logger.warning(
                f"Failed to fetch Confluence v2 page {page_id}: {e}. Retrying with v1 API."
            )
        try:
            page_resp = jira_client.get(
                f"wiki/rest/api/content/{page_id}?expand=body.storage,body.atlas_doc_format,version,space"
            )
            return page_resp.json()
        except (requests.RequestException, json.JSONDecodeError, KeyError) as err:
            if logger:
                logger.warning(f"Failed to fetch Confluence v1 page {page_id}: {err}")
            return None


def fetch_confluence_child_page_ids(
    jira_client: JiraClient, page_id: str, logger=None
) -> list[str]:
    """Fetches direct child page IDs for a given Confluence page ID."""
    child_ids: list[str] = []

    # Attempt Confluence REST API v2
    try:
        url = f"wiki/api/v2/pages/{page_id}/children"
        while url:
            resp = jira_client.get(url)
            data = resp.json()
            for item in data.get("results", []):
                cid = str(item.get("id", "")).strip()
                if cid and cid not in child_ids:
                    child_ids.append(cid)
            next_link = data.get("_links", {}).get("next")
            url = next_link if next_link else None
        return child_ids
    except (requests.RequestException, json.JSONDecodeError, KeyError) as e:
        if logger:
            logger.warning(
                f"Failed to fetch Confluence v2 children for page {page_id}: {e}. Retrying with v1 API."
            )

    # Fallback to Confluence REST API v1
    try:
        url = f"wiki/rest/api/content/{page_id}/child/page"
        while url:
            resp = jira_client.get(url)
            data = resp.json()
            for item in data.get("results", []):
                cid = str(item.get("id", "")).strip()
                if cid and cid not in child_ids:
                    child_ids.append(cid)
            next_link = data.get("_links", {}).get("next")
            url = next_link if next_link else None
        return child_ids
    except (requests.RequestException, json.JSONDecodeError, KeyError) as err:
        if logger:
            logger.warning(
                f"Failed to fetch Confluence v1 children for page {page_id}: {err}"
            )
        return child_ids


def parse_confluence_page_content(
    page_data: dict,
) -> tuple[str, str, dict | None]:
    """Extracts title, Markdown content, and ADF dict (if present) from Confluence page response."""
    title = page_data.get("title", "Untitled")
    body = page_data.get("body", {})

    # v2 format: body.atlas_doc_format.value
    adf_raw = body.get("atlas_doc_format", {}).get("value")
    if adf_raw:
        adf_dict = json.loads(adf_raw) if isinstance(adf_raw, str) else adf_raw
        return title, adf_to_markdown(adf_dict), adf_dict

    # v1 fallback: body.storage.value
    storage_raw = body.get("storage", {}).get("value", "")
    return title, storage_raw, None


def scrape_and_save_confluence_context(
    jira_client: JiraClient,
    remotelink_page_ids: list[str] | set[str],
    jira_id: str,
    logger=None,
) -> list[dict]:
    """Scrapes the remotelink document(s) and their child pages (without duplication),

    renders them to Markdown, and stores them in data/context/{jira_id}/.
    """
    # Collect remotelink documents and their direct children
    pages_to_scrape: list[str] = []
    seen_ids: set[str] = set()

    for pid in remotelink_page_ids:
        pid_str = str(pid).strip()
        if not pid_str:
            continue
        if pid_str not in seen_ids:
            seen_ids.add(pid_str)
            pages_to_scrape.append(pid_str)

        child_ids = fetch_confluence_child_page_ids(
            jira_client, pid_str, logger=logger
        )
        for cid in child_ids:
            if cid not in seen_ids:
                seen_ids.add(cid)
                pages_to_scrape.append(cid)

    safe_jira_id = (
        re.sub(r"[^\w\-]", "_", jira_id).strip("_") if jira_id else "context"
    )
    data_dir = Path("data")
    if not data_dir.exists() and Path("backend/data").exists():
        data_dir = Path("backend/data")
    context_dir = data_dir / "context" / safe_jira_id
    context_dir.mkdir(parents=True, exist_ok=True)

    saved_pages = []
    for page_id in pages_to_scrape:
        if logger:
            logger.info(f"Fetching Confluence page {page_id}...")

        page_data = fetch_confluence_page(jira_client, page_id, logger=logger)
        if not page_data:
            continue

        title, content_md, _ = parse_confluence_page_content(page_data)

        web_ui = page_data.get("_links", {}).get("webui", "")
        base_url = page_data.get("_links", {}).get("base", "")
        if not base_url:
            base_url = _get_base_url(jira_client)
        else:
            base_url = base_url.rstrip("/")

        if base_url and web_ui:
            page_url = f"{base_url}{web_ui}"
        elif base_url:
            page_url = f"{base_url}/wiki/pages/viewpage.action?pageId={page_id}"
        else:
            page_url = f"/wiki/pages/viewpage.action?pageId={page_id}"

        # Format and save Markdown file
        safe_title = (
            re.sub(r"[^\w\-]", "_", title).strip("_") or f"page_{page_id}"
        )
        file_name = f"{page_id}_{safe_title}.md"
        file_path = context_dir / file_name

        file_content = (
            f"# {title}\n\n"
            f"- **Page ID:** {page_id}\n"
            f"- **Confluence URL:** {page_url}\n\n"
            f"---\n\n"
            f"{content_md}\n"
        )

        with file_path.open("w", encoding="utf-8") as f:
            f.write(file_content)

        saved_pages.append(
            {
                "page_id": page_id,
                "title": title,
                "url": page_url,
                "file_path": str(file_path.resolve()),
                "content": content_md,
            }
        )

    return saved_pages


def load_existing_confluence_context(jira_id: str) -> list[dict]:
    """Loads existing Confluence context markdown files for an Epic if they already exist from another run."""
    safe_jira_id = (
        re.sub(r"[^\w\-]", "_", jira_id).strip("_") if jira_id else "context"
    )
    if not safe_jira_id or safe_jira_id == "context":
        return []

    data_dir = Path("data")
    if not data_dir.exists() and Path("backend/data").exists():
        data_dir = Path("backend/data")

    # Search candidate directories where epic context may reside
    candidate_dirs = [
        data_dir / "context" / safe_jira_id,
        data_dir / safe_jira_id,
        Path("data") / "context" / safe_jira_id,
        Path("data") / safe_jira_id,
        Path("backend/data") / "context" / safe_jira_id,
        Path("backend/data") / safe_jira_id,
    ]

    seen_dirs = set()
    for c_dir in candidate_dirs:
        try:
            resolved_c_dir = c_dir.resolve()
        except OSError:
            continue
        if resolved_c_dir in seen_dirs:
            continue
        seen_dirs.add(resolved_c_dir)

        if not c_dir.exists() or not c_dir.is_dir():
            continue

        md_files = [
            f
            for f in sorted(c_dir.glob("*.md"))
            if f.name != "test_plan.md" and not f.name.endswith("_test_plan.md")
        ]
        if not md_files:
            continue

        existing_pages: list[dict] = []
        for md_file in md_files:
            try:
                text = md_file.read_text(encoding="utf-8").strip()
                if not text:
                    continue

                title = md_file.stem
                page_id = ""
                page_url = ""
                content = text

                title_match = re.search(r"^#\s+(.+)$", text, flags=re.MULTILINE)
                if title_match:
                    title = title_match.group(1).strip()

                page_id_match = re.search(r"Page ID:\*\*?\s*(\d+)", text)
                if page_id_match:
                    page_id = page_id_match.group(1).strip()
                else:
                    stem_id_match = re.match(r"^(\d+)", md_file.stem)
                    if stem_id_match:
                        page_id = stem_id_match.group(1)

                url_match = re.search(r"Confluence URL:\*\*?\s*([^\n\r]+)", text)
                if url_match:
                    page_url = url_match.group(1).strip()

                if "---" in text:
                    content = text.split("---", 1)[1].strip()

                existing_pages.append(
                    {
                        "page_id": page_id,
                        "title": title,
                        "url": page_url,
                        "file_path": str(md_file.resolve()),
                        "content": content,
                    }
                )
            except OSError:
                continue

        if existing_pages:
            return existing_pages

    return []


@dg.asset
def jira_issue_epic(
    context: dg.AssetExecutionContext, jira_issue: dict, jira_client: JiraClient
):
    """Extracts the Epic link from a Jira issue, if present."""
    parent = jira_issue.get("fields", {}).get("parent")
    if parent and parent.get("fields", {}).get("issuetype", {}).get("name") == "Epic":
        self_link = parent.get("self")
        if self_link:
            response = jira_client.get(self_link)
            issue_data = response.json()
            fields = (
                issue_data.get("fields", {})
                if isinstance(issue_data, dict)
                else {}
            )
            status_field = fields.get("status")
            status_name = (
                status_field.get("name", "")
                if isinstance(status_field, dict)
                else ""
            )
            return dg.MaterializeResult(
                value=issue_data,
                metadata={
                    "jira_id": issue_data.get("key", ""),
                    "summary": fields.get("summary", ""),
                    "status": status_name,
                },
            )
    parent_key = parent.get("key", "") if isinstance(parent, dict) else ""
    return dg.MaterializeResult(
        value=None,
        metadata={"has_epic": False, "parent_key": parent_key},
    )


@dg.asset
def epic_requirements(
    context: dg.AssetExecutionContext,
    jira_issue_epic,
    jira_client: JiraClient,
):
    """Fetches Confluence remote links from the Epic, scrapes the remote link document

    and all its child pages (without duplication), and stores them as Markdown files
    in data/context/{jira_id}. If context for that Epic already exists from another run,
    reuses the existing context without recomputing.
    """
    jira_id = (jira_issue_epic and jira_issue_epic.get("key")) or "context"

    # Check if context already exists for this epic from a previous run
    if jira_id and jira_id != "context":
        existing_context = load_existing_confluence_context(jira_id)
        if existing_context:
            context.log.info(
                f"Context for Epic {jira_id} already exists ({len(existing_context)} documents). Reusing existing context without recomputing."
            )
            return dg.MaterializeResult(
                value=existing_context,
                metadata={
                    "count": len(existing_context),
                    "jira_id": jira_id,
                    "cached": True,
                    "page_titles": [p["title"] for p in existing_context],
                    "file_paths": [p["file_path"] for p in existing_context],
                },
            )

    remotelink_page_ids: set[str] = set()

    # Fetch remote links from Epic if available
    target_epic = jira_issue_epic or {}
    if target_epic.get("key"):
        try:
            epic_remotelinks_resp = jira_client.get(
                f"rest/api/3/issue/{target_epic['key']}/remotelink"
            )
            for link in epic_remotelinks_resp.json():
                url = link.get("object", {}).get("url", "")
                gid = link.get("globalId", "")
                remotelink_page_ids.update(_extract_page_ids_from_text(url))
                remotelink_page_ids.update(_extract_page_ids_from_text(gid))
        except (requests.RequestException, json.JSONDecodeError) as e:
            context.log.warning(
                f"Could not fetch remotelinks for Epic {target_epic.get('key')}: {e}"
            )

    # Scrape remote link page(s) and their child pages
    saved_pages = scrape_and_save_confluence_context(
        jira_client=jira_client,
        remotelink_page_ids=remotelink_page_ids,
        jira_id=jira_id,
        logger=context.log,
    )

    return dg.MaterializeResult(
        value=saved_pages,
        metadata={
            "count": len(saved_pages),
            "jira_id": jira_id,
            "cached": False,
            "page_titles": [p["title"] for p in saved_pages],
            "file_paths": [p["file_path"] for p in saved_pages],
        },
    )


@dg.asset
def processed_jira_issue(jira_issue):
    jira_id = jira_issue.get("key")
    summary = jira_issue.get("fields", {}).get("summary") or jira_issue.get(
        "summary", ""
    )
    description_node = jira_issue.get("fields", {}).get("description")
    description_text = adf_to_markdown(description_node)

    issue_url = ""
    self_url = jira_issue.get("self", "")
    if self_url and jira_id:
        match = re.match(r"(https?://[^/]+)", self_url)
        if match:
            issue_url = f"{match.group(1)}/browse/{jira_id}"
    if not issue_url and jira_id:
        base_url = os.getenv("BASE_URL", "").rstrip("/")
        if base_url:
            issue_url = f"{base_url}/browse/{jira_id}"
        else:
            issue_url = f"/browse/{jira_id}"

    return {
        "jira_id": jira_id,
        "title": summary,
        "description": description_text,
        "url": issue_url,
    }
