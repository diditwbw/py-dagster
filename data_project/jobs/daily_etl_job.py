# data_project/jobs/daily_etl_job.py
from dagster import job
#from dagster import define_asset_job

# Import aset yang ingin Anda sertakan dalam job ini
# Dalam kasus kita, kita hanya ingin menjalankan aset copy_incremental_data
#from ..assets.db_assets import copy_incremental_data
from ..repositories import copy_incremental_data

@job
def daily_copy_job():
    copy_incremental_data()

# Mendefinisikan Job untuk penyalinan data incremental harian

#daily_copy_job = define_asset_job(
#    name="daily_incremental_copy_job",  # Nama unik untuk job ini
#    selection=[copy_incremental_data],   # Pilih aset yang akan disertakan dalam job ini
#    description="Job harian untuk menyalin data incremental dari tabel sumber ke tabel tujuan."
#)

# Anda bisa menambahkan job lain di sini jika ada
# Contoh:
# from ..assets.transform_assets import transform_sales_data
# monthly_report_job = define_asset_job(
#     name="monthly_sales_report_job",
#     selection=[transform_sales_data],
#     description="Job bulanan untuk menghasilkan laporan penjualan."
# )