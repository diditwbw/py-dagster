# data_project/__init__.py
from .repositories import defs

# This makes the defs available when Dagster imports data_project
__all__ = ["defs"]