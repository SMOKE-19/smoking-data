"""Packaged Spotfire function catalog resources."""

from __future__ import annotations

from importlib import resources
from pathlib import Path


def catalog_db_path() -> Path:
    """Return the packaged SQLite catalog path when available on disk."""
    return Path(
        str(
            resources.files("spotfire_expr_normalizer.data").joinpath(
                "spotfire_function_catalog.sqlite"
            )
        )
    )


def unsupported_functions_markdown_path() -> Path:
    """Return the packaged unsupported-function markdown report path."""
    return Path(
        str(
            resources.files("spotfire_expr_normalizer.data").joinpath(
                "ETL0202_SPOTFIRE_UNSUPPORTED_FUNCTIONS.md"
            )
        )
    )
