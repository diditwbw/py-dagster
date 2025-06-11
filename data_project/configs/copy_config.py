# data_project/configs/copy_config.py --> LAMA

#from dagster import Config
#
#class CopyConfig(Config):
#    source_table: str
#    dest_table: str
#    incremental_column: str # Nama kolom timestamp/ID di tabel sumber
#    filter_by_type: str = "timestamp" # "timestamp" atau "id"
#    checkpoint_process_name: str # Nama proses di tabel ETL_Checkpoint
    
    
# data_project/configs/copy_config.py --> BARU
from dagster import ConfigurableResource
from pydantic import Field
from typing import Literal

class CopyConfig(ConfigurableResource):
    source_table: str
    dest_table: str
    incremental_column: str
    filter_by_type: Literal["timestamp", "id"] #str
    checkpoint_process_name: str