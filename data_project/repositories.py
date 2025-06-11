# data_project/repositories.py
import os
import pandas as pd
from sqlalchemy import create_engine, text

from dotenv import load_dotenv
from dagster import Definitions, asset, OpExecutionContext, Config

load_dotenv()
from .assets.db_assets import source_data_table, destination_data_table, copy_incremental_data
from .resources.db_io import DbConfig
from .configs.copy_config import CopyConfig
from .jobs.daily_etl_job import daily_copy_job
from .schedules.daily_schedule import daily_incremental_copy_schedule



# Definitions object
defs = Definitions(
    assets=[
        source_data_table,
        destination_data_table,
        copy_incremental_data
    ],
    jobs=[daily_copy_job],
    schedules=[daily_incremental_copy_schedule],
    resources={
        "source_db_config": DbConfig(
            server=os.getenv("SOURCE_DB_SERVER"),
            database=os.getenv("SOURCE_DB_DATABASE"),
            username=os.getenv("SOURCE_DB_USERNAME") if os.getenv("SOURCE_DB_USERNAME") else None,
            password=os.getenv("SOURCE_DB_PASSWORD") if os.getenv("SOURCE_DB_PASSWORD") else None,
            use_windows_auth=bool(os.getenv("SOURCE_DB_USERNAME") is None)
        ),
        "dest_db_config": DbConfig(
            server=os.getenv("DEST_DB_SERVER"),
            database=os.getenv("DEST_DB_DATABASE"),
            username=os.getenv("DEST_DB_USERNAME") if os.getenv("DEST_DB_USERNAME") else None,
            password=os.getenv("DEST_DB_PASSWORD") if os.getenv("DEST_DB_PASSWORD") else None,
            use_windows_auth=bool(os.getenv("DEST_DB_USERNAME") is None)
        ),
        "copy_config": CopyConfig(
            source_table="PayrollData",
            dest_table="PayrollData",
            incremental_column="ModifiedDate",
            filter_by_type="timestamp",
            checkpoint_process_name="CopyPayrollData"
        )
    }
)