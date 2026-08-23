from __future__ import annotations

import shutil
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py


class WorkspaceBuildPy(build_py):
    """Bundle the repository-level workspace source into the Python wheel."""

    def run(self) -> None:
        super().run()
        project_root = Path(__file__).resolve().parent
        source = project_root / "workspace"
        if not source.is_dir():
            raise FileNotFoundError(f"workspace source directory not found: {source}")

        package_root = Path(self.build_lib) / "smoking_data"
        legacy_sbdf_package = Path(self.build_lib) / "streaming_sbdf_rs"
        if legacy_sbdf_package.exists():
            shutil.rmtree(legacy_sbdf_package)
        packaged_target = package_root / "_workspace"
        if packaged_target.exists():
            shutil.rmtree(packaged_target)
        for legacy_name in ("workspace", "workspace_template"):
            legacy_target = package_root / legacy_name
            if legacy_target.exists():
                shutil.rmtree(legacy_target)
        shutil.copytree(source, packaged_target)


setup(cmdclass={"build_py": WorkspaceBuildPy})
