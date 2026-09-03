"""Composable, bounded-memory Asset production engine."""

from __future__ import annotations

__version__ = "0.1.19"

from smoking_data.api import ValidationResult, validate_definition
from smoking_data.runtime.capabilities import get_capabilities

__all__ = ["ValidationResult", "validate_definition", "get_capabilities", "__version__"]
