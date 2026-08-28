"""SPI-specific workspace initialization CLI."""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import sys
from importlib import resources
from pathlib import Path
from typing import Sequence

from smoking_data.cli import main as _smoking_data_main


def main(argv: Sequence[str] | None = None) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    if not args or args[0] in {"-h", "--help"}:
        print("usage: smoking-data-cli-spi init [TARGET] [--force] [--json]")
        return 0
    if args[0] != "init":
        print("smoking-data-cli-spi는 init 명령만 제공합니다.")
        return 2

    parser = argparse.ArgumentParser(description="Initialize an SPI-specific smoking-data workspace.")
    parser.add_argument("target", nargs="?", default=".")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--json", action="store_true")
    parsed = parser.parse_args(args[1:])

    init_args = ["init", parsed.target]
    if parsed.force:
        init_args.append("--force")
    init_args.append("--json")
    init_stdout = io.StringIO()
    with contextlib.redirect_stdout(init_stdout):
        result = _smoking_data_main(init_args)
    if result != 0:
        return result

    # SPI override files are package-owned: the SPI initializer must apply them
    # even when the general init keeps user-owned default templates.
    overlay = _overlay_workspace(parsed.target, force=True)
    if parsed.json:
        try:
            init_payload = json.loads(init_stdout.getvalue())
        except json.JSONDecodeError:
            init_payload = {"ok": True, "output": init_stdout.getvalue().strip()}
        print(
            json.dumps(
                {"ok": True, "command": "init", "init": init_payload, "spi_overlay": overlay},
                ensure_ascii=False,
            )
        )
    else:
        print(init_stdout.getvalue(), end="")
        print(f"[smoking-data-cli-spi] workspace initialized; overlay={len(overlay['updated'])}")
    return 0


def _overlay_workspace(target: str | Path, *, force: bool) -> dict[str, list[str]]:
    root = Path(target).expanduser().resolve()
    source = resources.files("smoking_data_cli_spi").joinpath("_workspace")
    updated: list[str] = []
    preserved: list[str] = []

    def visit(node, relative: Path) -> None:
        for child in node.iterdir():
            child_relative = relative / child.name
            if child.is_dir():
                visit(child, child_relative)
                continue
            destination_relative = _workspace_destination(child_relative)
            destination = root / destination_relative
            if destination.exists() and not force:
                preserved.append(destination_relative.as_posix())
                continue
            destination.parent.mkdir(parents=True, exist_ok=True)
            temporary = destination.with_suffix(destination.suffix + ".tmp")
            temporary.write_bytes(child.read_bytes())
            temporary.replace(destination)
            updated.append(destination_relative.as_posix())

    if source.is_dir():
        visit(source, Path())
    return {"updated": updated, "preserved": preserved}


def _workspace_destination(relative: Path) -> Path:
    aliases = {"vscode": ".vscode", "smoking_data": ".smoking-data", "agent": ".agent"}
    parts = list(relative.parts)
    if parts and parts[0] in aliases:
        parts[0] = aliases[parts[0]]
    return Path(*parts)
