from __future__ import annotations

TYPE_RULE_VERSION = "streaming-sbdf-name-rules.v1"


def name_type_rule(column_name: str) -> str | None:
    """Return the shared streaming-SBDF name rule for a CSV column.

    Explicit ``columns_by_type`` entries are resolved before this function is
    called.  The rules intentionally stay small and deterministic: they are
    the same durable name hints used by the SBDF CSV/dataframe path.
    """

    normalized = column_name.casefold()
    if normalized == "wafer_id":
        return "INT64"
    if "time" in normalized:
        return "DATETIME"
    return None
