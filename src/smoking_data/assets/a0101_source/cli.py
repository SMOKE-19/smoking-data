from __future__ import annotations

import argparse
import json

from .runner import execute_yaml


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run a windowed source query and write Parquet datasets."
    )
    parser.add_argument("yaml_path")
    args = parser.parse_args(argv)
    result = execute_yaml(args.yaml_path)
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
