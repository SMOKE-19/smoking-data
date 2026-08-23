"""Cross-Asset Parquet physical-layout recommendations."""

from .advisor import PhysicalLayoutReport, generate_physical_layout_report

__all__ = ["PhysicalLayoutReport", "generate_physical_layout_report"]
