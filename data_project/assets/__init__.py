# data_project/assets/__init__.py
from .db_assets import source_data_table, destination_data_table, copy_incremental_data

__all__ = ["source_data_table", "destination_data_table", "copy_incremental_data"]