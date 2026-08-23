from __future__ import annotations

import json
import shutil
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = PROJECT_ROOT / "workspace"
EXAMPLES = WORKSPACE / "examples"
SCHEMAS = PROJECT_ROOT / "schemas"
VSCODE = WORKSPACE / "vscode"


def _example_identity(path: Path) -> tuple[str, str]:
    parts = path.name.removesuffix(".yaml").split(".")
    if len(parts) < 3:
        raise ValueError(f"Example filename does not follow the Asset convention: {path.name}")
    asset_code = parts[-1]
    description = "-".join(parts[1:-1]).replace("_", "-")
    return asset_code, description


def _example_prefix(path: Path, *, full: bool) -> str:
    asset_code, description = _example_identity(path)
    suffix = "-full" if full else ""
    return f"sd-example-{asset_code}-{description}{suffix}"


def _slim_example_text(path: Path) -> str:
    source_text = path.read_text(encoding="utf-8")
    payload = yaml.safe_load(source_text)
    if not isinstance(payload, dict):
        raise TypeError(f"Example YAML root must be a mapping: {path}")
    asset_code, _ = _example_identity(path)
    if asset_code == "0101":
        payload.pop("execution", None)
        payload["output"] = {}
    elif asset_code in {"0102", "0201", "0301", "0401"}:
        lines = source_text.rstrip().splitlines()
        lines = _replace_yaml_section(lines, marker="output:", replacement=["output: {}"])
        lines = _replace_yaml_section(lines, marker="execution:", replacement=[])
        return "\n".join(lines).rstrip()
    elif asset_code == "chain":
        payload.pop("execution", None)
    return yaml.safe_dump(payload, allow_unicode=True, sort_keys=False).rstrip()


def _replace_yaml_section(
    lines: list[str],
    *,
    marker: str,
    replacement: list[str],
) -> list[str]:
    try:
        start = lines.index(marker)
    except ValueError:
        return lines
    indentation = len(marker) - len(marker.lstrip())
    end = len(lines)
    for index in range(start + 1, len(lines)):
        stripped = lines[index].lstrip()
        if not stripped or stripped.startswith("#"):
            continue
        current_indentation = len(lines[index]) - len(stripped)
        if current_indentation <= indentation:
            end = index
            break
    return [*lines[:start], *replacement, *lines[end:]]


def _generated_snippet(path: Path, *, full: bool) -> dict[str, object]:
    text = path.read_text(encoding="utf-8").rstrip() if full else _slim_example_text(path)
    text = "\n".join(
        line for line in text.splitlines() if not line.strip().startswith("output_dtype:")
    )
    variant = "전체" if full else "Slim"
    return {
        "scope": "yaml",
        "prefix": _example_prefix(path, full=full),
        "description": f"{path.name} {variant} 예시",
        "body": [*text.splitlines(), "$0"],
    }


def _sync_snippets(target: Path, examples: list[Path]) -> None:
    payload = json.loads(target.read_text(encoding="utf-8"))
    payload = {
        name: snippet
        for name, snippet in payload.items()
        if not (
            isinstance(snippet, dict) and str(snippet.get("prefix", "")).startswith("sd-example-")
        )
    }
    for path in examples:
        payload[f"Smoking Data Example · {path.name} · Slim"] = _generated_snippet(path, full=False)
        payload[f"Smoking Data Example · {path.name} · Full"] = _generated_snippet(path, full=True)
    target.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _copy_schema(name: str, target_root: Path) -> None:
    target_root.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(SCHEMAS / name, target_root / name)


def main() -> None:
    source_examples = sorted(EXAMPLES.glob("*.0101.yaml"))
    engine_examples = sorted(
        path
        for suffix in ("0102", "0201", "0301", "0401", "chain")
        for path in EXAMPLES.glob(f"*.{suffix}.yaml")
    )
    _sync_snippets(VSCODE / "smoking-data-0101-source.code-snippets", source_examples)
    _sync_snippets(VSCODE / "engine.code-snippets", engine_examples)

    (VSCODE / "schemas" / "probe-v3.schema.json").unlink(missing_ok=True)
    (VSCODE / "schemas" / "probe-v4.schema.json").unlink(missing_ok=True)
    (VSCODE / "schemas" / "probe-v5.schema.json").unlink(missing_ok=True)
    (VSCODE / "schemas" / "probe-v6.schema.json").unlink(missing_ok=True)
    (VSCODE / "schemas" / "asset-contract-v1.schema.json").unlink(missing_ok=True)
    (VSCODE / "schemas" / "asset-config-v2.schema.json").unlink(missing_ok=True)

    for name in (
        "source-0101.schema.json",
        "asset-config-v3.schema.json",
        "pipeline-v6.schema.json",
        "pipeline-v7.schema.json",
        "calculated-fact-v2.schema.json",
        "asset-chain-v2.schema.json",
        "schedule-v1.schema.json",
        "layout-migration-v1.schema.json",
        "physical-layout-recommendation-v2.schema.json",
    ):
        _copy_schema(name, VSCODE / "schemas")


if __name__ == "__main__":
    main()
