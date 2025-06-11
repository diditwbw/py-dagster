# data_project/resources/db_io.py
import urllib
from dagster import ConfigurableResource
from pydantic import Field

class DbConfig(ConfigurableResource):
    server: str
    database: str
    username: str = Field(default=None)
    password: str = Field(default=None)
    use_windows_auth: bool = Field(default=False)

    def get_conn_str(self):
        if self.use_windows_auth:
            return f"mssql+pyodbc://{self.server}/{self.database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
        else:
            quoted_password = urllib.parse.quote_plus(self.password)
            return f"mssql+pyodbc://{self.username}:{quoted_password}@{self.server}/{self.database}?driver=ODBC+Driver+17+for+SQL+Server"