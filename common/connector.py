import pyodbc
import pandas as pd
from .config import SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASS, SQL_DRIVER, TIMEOUT_S

def call_stored_procedure(sp_name: str, params: dict = None) -> pd.DataFrame:
    """
    Ejecuta un Stored Procedure con parámetros en SQL Server y devuelve un DataFrame.
    Ejemplo:
        call_stored_procedure("dbo.GetExpectedProducts", {"@Marca": "HAVANNA"})
    """
    conn_str = (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};PWD={SQL_PASS};TrustServerCertificate=yes;"
    )

    with pyodbc.connect(conn_str, timeout=TIMEOUT_S) as conn:
        # placeholders tipo "@param = ?" para SP parametrizado
        param_str = ", ".join(f"{k}=?" for k in params) if params else ""
        sql = f"EXEC {sp_name} {param_str}"
        values = tuple(params.values()) if params else ()
        df = pd.read_sql(sql, conn, params=values)
    return df
