# data_project/schedules/daily_schedule.py

from dagster import ScheduleDefinition

# Import job yang ingin dijadwalkan dari modul jobs
from ..jobs.daily_etl_job import daily_copy_job


# Definisi jadwal harian
daily_incremental_copy_schedule = ScheduleDefinition(
    job=daily_copy_job,                      # Job yang akan dijalankan oleh jadwal ini
    cron_schedule="30 15 * * *",               # Jadwal cron: setiap hari jam 01:00 (1 pagi)
    execution_timezone="Asia/Jakarta",       # Zona waktu eksekusi (sesuaikan jika perlu)
    name="daily_incremental_copy_schedule",  # Nama unik untuk jadwal di Dagster UI
    description="Jadwal harian untuk menyalin data incremental dari tabel Orders."
)

# Anda bisa menambahkan jadwal lain di sini jika ada
# daily_other_job_schedule = ScheduleDefinition(
#     job=daily_other_job,
#     cron_schedule="0 2 * * *",
#     execution_timezone="Asia/Jakarta",
#     name="daily_other_job_schedule",
#     description="Jadwal untuk job lainnya."
# )