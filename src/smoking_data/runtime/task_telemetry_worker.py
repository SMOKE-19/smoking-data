from __future__ import annotations

import argparse
import json
import os
import socket
from pathlib import Path

from smoking_data.runtime.task_telemetry import _supervisor_loop


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--token", required=True)
    parser.add_argument("--log-path", type=Path, required=True)
    parser.add_argument("--ready-path", type=Path, required=True)
    parser.add_argument("--summary-path", type=Path, required=True)
    parser.add_argument("--sample-interval-sec", type=float, required=True)
    parser.add_argument("--console-progress", choices=("off", "plain", "tty"), default="off")
    parser.add_argument("--progress-title", default="smoking-data")
    args = parser.parse_args()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver:
            receiver.bind(("127.0.0.1", 0))
            receiver.settimeout(args.sample_interval_sec)
            host, port = receiver.getsockname()
            _write_json_atomic(
                args.ready_path,
                {"status": "ready", "host": host, "port": port},
            )
            profile = _supervisor_loop(
                receiver,
                token=args.token,
                log_path=args.log_path,
                sample_interval_sec=args.sample_interval_sec,
                console_progress=args.console_progress,
                progress_title=args.progress_title,
            )
        _write_json_atomic(args.summary_path, profile)
        return 0
    except BaseException as exc:
        _write_json_atomic(
            args.summary_path,
            {"status": "report_failed", "failure_reason": f"{type(exc).__name__}: {exc}"},
        )
        return 1


def _write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(temporary, path)


if __name__ == "__main__":
    raise SystemExit(main())
