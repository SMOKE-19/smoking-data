"""0401 analysis snapshot Asset producer."""

from pathlib import Path

from smoking_data.core.results import StageResult


def run_yaml(
    yaml_path: str | Path,
    *,
    config_path: str | Path | None = None,
    project_root: str | Path | None = None,
) -> StageResult:
    """Run the canonical Pipeline v8 definition that produces one 0401 snapshot file."""
    from smoking_data.runtime.runner import run_pipeline_yaml

    return run_pipeline_yaml(
        yaml_path,
        config_path=config_path,
        project_root=project_root,
    )


__all__ = ["run_yaml"]
