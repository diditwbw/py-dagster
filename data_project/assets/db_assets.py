# data_project/assets/db_assets.py
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from dagster import asset, AssetExecutionContext

@asset(
    name="source_data_table",
    required_resource_keys={"source_db_config"}
)
def source_data_table(context: AssetExecutionContext):
    source_db_config = context.resources.source_db_config
    context.log.info(f"Mengakses tabel sumber: {source_db_config.server}/{source_db_config.database}")
    return "source_data_accessible"

@asset(
    name="destination_data_table",
    required_resource_keys={"dest_db_config"}
)
def destination_data_table(context: AssetExecutionContext):
    dest_db_config = context.resources.dest_db_config
    context.log.info(f"Menargetkan tabel tujuan: {dest_db_config.server}/{dest_db_config.database}")
    return "destination_data_accessible"

@asset(
    name="copy_incremental_data",
    deps=["source_data_table", "destination_data_table"],
    required_resource_keys={"source_db_config", "dest_db_config", "copy_config"}
)
def copy_incremental_data(context: AssetExecutionContext):
    source_db_config = context.resources.source_db_config
    dest_db_config = context.resources.dest_db_config
    copy_config = context.resources.copy_config
    
    context.log.info(f"Memulai proses penyalinan incremental untuk '{copy_config.checkpoint_process_name}'...")

    source_engine = create_engine(source_db_config.get_conn_str())
    dest_engine = create_engine(dest_db_config.get_conn_str())
    
    # ----- TAMBAHKAN LOG INI -----
    #last_checkpoint_value = "2025-01-01" # Ini harus berasal dari logic Anda
    #context.log.info(f"Using last_checkpoint_value: {last_checkpoint_value}")
#
    #sql_read_query = "SELECT * FROM [dbo.PayrollData] WHERE [ModifiedDate] > ? ORDER BY [ModifiedDate] ASC"
    #context.log.info(f"Executing SQL read query: {sql_read_query} with param: {last_checkpoint_value}")
    # ---------------------------

    last_checkpoint_value = None
    max_value_in_batch = None
    rows_copied = 0
    
    

    try:
        # Your existing logic here - I'll keep it short for brevity
        # ... (copy all your incremental data logic from repositories.py)
        
        # 1. Get checkpoint
        with source_engine.connect() as conn:
            query_checkpoint = text(
                f"SELECT LastCopiedTimestamp, LastCopiedId "
                f"FROM PayrollData WHERE ProcessName = :process_name"
            )
            result = conn.execute(query_checkpoint, {"process_name": copy_config.checkpoint_process_name}).fetchone()

            if result:
                if copy_config.filter_by_type == "timestamp":
                    last_checkpoint_value = result.LastCopiedTimestamp
                    if last_checkpoint_value:
                        last_checkpoint_value -= timedelta(seconds=1)
                else:
                    last_checkpoint_value = result.LastCopiedId
            else:
                context.log.warning(f"Checkpoint untuk '{copy_config.checkpoint_process_name}' tidak ditemukan.")
                if copy_config.filter_by_type == "timestamp":
                    last_checkpoint_value = datetime(1900, 1, 1)
                else:
                    last_checkpoint_value = 0
            
            context.log.info(f"Checkpoint terakhir: {last_checkpoint_value}")
        
        with source_engine.connect() as conn:  # <- Also change the update
            with conn.begin():
                if copy_config.filter_by_type == "timestamp":
                    update_query = text(
                        "UPDATE PayrollData SET LastCopiedTimestamp = :new_value "
                        "WHERE ProcessName = :process_name"
                    )
                else:
                    update_query = text(
                        "UPDATE PayrollData SET LastCopiedId = :new_value "
                        "WHERE ProcessName = :process_name"
                    )
                conn.execute(update_query, {
                    "new_value": max_value_in_batch,
                    "process_name": copy_config.checkpoint_process_name
                })
                conn.commit()

        # 2. Read new data
        if copy_config.filter_by_type == "timestamp":
            sql_read_query = text(
                f"SELECT * FROM [{copy_config.source_table}] "
                f"WHERE [{copy_config.incremental_column}] > :last_value "
                f"ORDER BY [{copy_config.incremental_column}] ASC"
            )
        else:
            sql_read_query = text(
                f"SELECT * FROM [{copy_config.source_table}] "
                f"WHERE [{copy_config.incremental_column}] > :last_value "
                f"ORDER BY [{copy_config.incremental_column}] ASC"
            )
        
        df = pd.read_sql(sql_read_query, source_engine, params={"last_value": last_checkpoint_value})
        rows_copied = len(df)
        context.log.info(f"Ditemukan {rows_copied} baris baru untuk disalin.")

        if rows_copied > 0:
            max_value_in_batch = df[copy_config.incremental_column].max()
            context.log.info(f"Nilai max di batch: {max_value_in_batch}")

            df.to_sql(copy_config.dest_table, dest_engine, if_exists='append', index=False)
            context.log.info("Penyalinan data berhasil!")

            # Update checkpoint
            with dest_engine.connect() as conn:
                with conn.begin():
                    if copy_config.filter_by_type == "timestamp":
                        update_query = text(
                            f"UPDATE PayrollData SET LastCopiedTimestamp = :new_value "
                            f"WHERE ProcessName = :process_name"
                        )
                    else:
                        update_query = text(
                            f"UPDATE PayrollData SET LastCopiedId = :new_value "
                            f"WHERE ProcessName = :process_name"
                        )
                    conn.execute(update_query, {
                        "new_value": max_value_in_batch,
                        "process_name": copy_config.checkpoint_process_name
                    })
                    conn.commit()
            context.log.info(f"Checkpoint diperbarui ke {max_value_in_batch}.")
        else:
            context.log.info("Tidak ada data baru untuk disalin.")

        return f"Copied {rows_copied} rows successfully"

    except Exception as e:
        context.log.error(f"Terjadi kesalahan: {e}")
        raise
    finally:
        if source_engine: source_engine.dispose()
        if dest_engine: dest_engine.dispose()