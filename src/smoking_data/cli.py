from __future__ import annotations

from .runtime.runner import main as runner_main


def main(argv: list[str] | None = None) -> int:
    return runner_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
