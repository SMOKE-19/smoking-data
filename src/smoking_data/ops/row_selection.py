from __future__ import annotations

from typing import Any

import polars as pl


def apply_sort_first(
    lf: pl.LazyFrame,
    *,
    group_keys: list[str],
    sort: list[dict[str, Any]],
) -> pl.LazyFrame:
    if not group_keys:
        return lf
    sort_columns = [str(item.get("column") or "").strip() for item in sort]
    descending = [str(item.get("direction") or "asc").lower() == "desc" for item in sort]
    if any(not column for column in sort_columns):
        raise ValueError("sort_first.sort items must define column.")
    if not sort_columns:
        return lf.unique(subset=group_keys, keep="first", maintain_order=True)
    return (
        lf.sort(sort_columns, descending=descending)
        .group_by(group_keys, maintain_order=True)
        .first()
    )
