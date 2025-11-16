from __future__ import annotations

"""
Support utilities for Site Reliability Engineering (SRE) insights.

To avoid import side-effects (e.g. while running `python -m sre_analysis.online_monitor`),
the public objects are loaded lazily via __getattr__.
"""

from importlib import import_module
from typing import Any

__all__ = ["OnlineMonitor", "MonitorResult", "OnlineSnapshot", "run_post_ingestion_monitor"]


def __getattr__(name: str) -> Any:
    if name in __all__:
        module = import_module(".online_monitor", __name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__} has no attribute {name}")

