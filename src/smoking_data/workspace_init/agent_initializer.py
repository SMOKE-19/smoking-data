from __future__ import annotations

import stat
from pathlib import Path
from typing import Any

from .template_resources import template_resource

_AGENT_GUIDANCE_START = "<!-- smoking-data:agent-guidance:start -->"
_AGENT_GUIDANCE_END = "<!-- smoking-data:agent-guidance:end -->"

_MANAGED_AGENT_FILES = (
    Path("README.md"),
    Path("smoking-data/manifest.yaml"),
    Path("smoking-data/CLI.md"),
    Path("smoking-data/METADATA_MAP.md"),
    Path("smoking-data/PROFILE_ANALYSIS.md"),
    Path("smoking-data/FAILURE_DIAGNOSIS.md"),
    Path("smoking-data/MISSING_DATA_DIAGNOSIS.md"),
    Path("smoking-data/REPORT_FORMAT.md"),
    Path("smoking-data/SANDBOX.md"),
)


def initialize_agent_workspace(target: str | Path) -> dict[str, Any]:
    workspace_root = Path(target).expanduser().resolve()
    workspace_root.mkdir(parents=True, exist_ok=True)
    created: list[str] = []
    updated: list[str] = []
    unchanged: list[str] = []
    preserved: list[str] = []

    agents_path = workspace_root / "AGENTS.md"
    agents_result = _sync_agents_entrypoint(agents_path)
    agents_action = agents_result["action"]
    if agents_action == "created":
        created.append("AGENTS.md")
    elif agents_action in {"appended", "updated_managed_block"}:
        updated.append("AGENTS.md")
        preserved.append("AGENTS.md")
    elif agents_action == "unchanged":
        unchanged.append("AGENTS.md")
        if agents_result["preexisting"]:
            preserved.append("AGENTS.md")
    else:
        preserved.append("AGENTS.md")

    agent_root = workspace_root / ".agent"
    for relative in _MANAGED_AGENT_FILES:
        target_path = agent_root / relative
        state = _sync_resource(template_resource("agent", *relative.parts), target_path)
        {"created": created, "updated": updated, "unchanged": unchanged}[state].append(
            target_path.relative_to(workspace_root).as_posix()
        )

    local_context = agent_root / "local" / "CONTEXT.md"
    if local_context.exists():
        if not local_context.is_file():
            raise ValueError(f"Agent local context가 파일이 아닙니다: {local_context}")
        preserved.append(local_context.relative_to(workspace_root).as_posix())
    else:
        _write_resource(template_resource("agent", "local", "CONTEXT.md"), local_context)
        created.append(local_context.relative_to(workspace_root).as_posix())

    sandbox_root = workspace_root / "for_agents"
    for relative in (Path("scripts"), Path("output")):
        directory = sandbox_root / relative
        if directory.exists():
            if not directory.is_dir():
                raise ValueError(f"Agent sandbox 경로가 디렉터리가 아닙니다: {directory}")
            preserved.append(directory.relative_to(workspace_root).as_posix())
        else:
            directory.mkdir(parents=True, exist_ok=False)
            created.append(directory.relative_to(workspace_root).as_posix())

    gitignore = sandbox_root / ".gitignore"
    state = _sync_text("*\n", gitignore)
    {"created": created, "updated": updated, "unchanged": unchanged}[state].append(
        gitignore.relative_to(workspace_root).as_posix()
    )

    return {
        "ok": True,
        "workspace_root": str(workspace_root),
        "agents_path": str(agents_path),
        "agent_root": str(agent_root),
        "sandbox_root": str(sandbox_root),
        "agents_entrypoint_preserved": agents_result["preexisting"],
        "agents_entrypoint_action": agents_action,
        "agents_entrypoint_reason": agents_result.get("reason"),
        "agents_entrypoint_links_guidance": agents_result["links_guidance"],
        "manual_link_required": agents_result["manual_link_required"],
        "created": created,
        "updated": updated,
        "unchanged": unchanged,
        "preserved": preserved,
    }


def _sync_agents_entrypoint(target: Path) -> dict[str, Any]:
    managed_bytes = template_resource("AGENTS.md").read_bytes()
    try:
        managed_block = managed_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:  # pragma: no cover - package resource invariant
        raise ValueError("AGENTS.md package resource must be UTF-8.") from exc
    _validate_managed_agents_block(managed_block)

    if not target.exists() and not target.is_symlink():
        _write_resource(template_resource("AGENTS.md"), target)
        return {
            "action": "created",
            "preexisting": False,
            "links_guidance": True,
            "manual_link_required": False,
        }

    if target.is_symlink():
        return _skipped_agents_result(target, "symbolic_link")
    if not target.is_file():
        return _skipped_agents_result(target, "not_regular_file")

    original_bytes = target.read_bytes()
    try:
        original = original_bytes.decode("utf-8")
    except UnicodeDecodeError:
        return {
            "action": "skipped",
            "preexisting": True,
            "reason": "not_utf8",
            "links_guidance": False,
            "manual_link_required": True,
        }

    start_count = original.count(_AGENT_GUIDANCE_START)
    end_count = original.count(_AGENT_GUIDANCE_END)
    if start_count or end_count:
        if start_count != 1 or end_count != 1:
            return {
                "action": "skipped",
                "preexisting": True,
                "reason": "invalid_managed_markers",
                "links_guidance": _agents_links_guidance(original),
                "manual_link_required": not _agents_links_guidance(original),
            }
        start = original.index(_AGENT_GUIDANCE_START)
        end = original.index(_AGENT_GUIDANCE_END) + len(_AGENT_GUIDANCE_END)
        if start >= end:
            return {
                "action": "skipped",
                "preexisting": True,
                "reason": "invalid_managed_markers",
                "links_guidance": _agents_links_guidance(original),
                "manual_link_required": not _agents_links_guidance(original),
            }
        newline = _detect_newline(original)
        replacement = managed_block.replace("\n", newline).rstrip("\r\n")
        updated = original[:start] + replacement + original[end:]
        if updated == original:
            return {
                "action": "unchanged",
                "preexisting": True,
                "links_guidance": True,
                "manual_link_required": False,
            }
        _replace_text_preserving_mode(target, updated)
        return {
            "action": "updated_managed_block",
            "preexisting": True,
            "links_guidance": True,
            "manual_link_required": False,
        }

    if _agents_links_guidance(original):
        return {
            "action": "unchanged",
            "preexisting": True,
            "links_guidance": True,
            "manual_link_required": False,
        }

    newline = _detect_newline(original)
    replacement = managed_block.replace("\n", newline).rstrip("\r\n")
    existing = original.rstrip("\r\n")
    updated = (existing + newline * 2 if existing else "") + replacement + newline
    _replace_text_preserving_mode(target, updated)
    return {
        "action": "appended",
        "preexisting": True,
        "links_guidance": True,
        "manual_link_required": False,
    }


def _validate_managed_agents_block(content: str) -> None:
    if (
        content.count(_AGENT_GUIDANCE_START) != 1
        or content.count(_AGENT_GUIDANCE_END) != 1
        or content.index(_AGENT_GUIDANCE_START) >= content.index(_AGENT_GUIDANCE_END)
    ):
        raise ValueError("AGENTS.md package resource must contain one managed marker block.")


def _agents_links_guidance(content: str) -> bool:
    return ".agent/README.md" in content or ".agent/smoking-data" in content


def _detect_newline(content: str) -> str:
    return "\r\n" if "\r\n" in content else "\n"


def _replace_text_preserving_mode(target: Path, content: str) -> None:
    mode = stat.S_IMODE(target.stat().st_mode)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_bytes(content.encode("utf-8"))
    temporary.chmod(mode)
    temporary.replace(target)


def _skipped_agents_result(target: Path, reason: str) -> dict[str, Any]:
    links_guidance = False
    try:
        if target.is_file():
            links_guidance = _agents_links_guidance(target.read_text(encoding="utf-8"))
    except (OSError, UnicodeError):
        pass
    return {
        "action": "skipped",
        "preexisting": True,
        "reason": reason,
        "links_guidance": links_guidance,
        "manual_link_required": not links_guidance,
    }


def _sync_resource(resource: Any, target: Path) -> str:
    content = resource.read_bytes()
    if target.is_file() and target.read_bytes() == content:
        return "unchanged"
    state = "updated" if target.exists() else "created"
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_bytes(content)
    temporary.replace(target)
    return state


def _write_resource(resource: Any, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_bytes(resource.read_bytes())
    temporary.replace(target)


def _sync_text(content: str, target: Path) -> str:
    encoded = content.encode("utf-8")
    if target.is_file() and target.read_bytes() == encoded:
        return "unchanged"
    state = "updated" if target.exists() else "created"
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_bytes(encoded)
    temporary.replace(target)
    return state
