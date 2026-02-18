import os
import sys
import pyodbc
import pandas as pd
import datetime
from pathlib import Path
from dotenv import load_dotenv

# ---------------------------------------------------------
# Asegura que el módulo raíz 'ctcVendorController' esté disponible
# ---------------------------------------------------------
BASE_DIR = Path(__file__).resolve()
while BASE_DIR.name != "ctcVendorController":
    BASE_DIR = BASE_DIR.parent
    if BASE_DIR.parent == BASE_DIR:
        raise FileNotFoundError("No se encontró la carpeta raíz 'ctcVendorController'.")

if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from common.config import CATALOG_ID, PROVIDER_ID_LIST, settings

# ---------------------------------------------------------
# Carga de variables confidenciales desde .env
# ---------------------------------------------------------
ENV_PATH = BASE_DIR / ".env"

if not ENV_PATH.exists():
    raise FileNotFoundError(f"No se encontró el archivo .env en: {ENV_PATH}")

load_dotenv(dotenv_path=ENV_PATH)
print(f"Archivo .env cargado desde: {ENV_PATH}")

server = settings.DB_SERVER
database = settings.DB_NAME
username = settings.DB_USER
password = settings.DB_PASSWORD

if not all([server, database, username, password]):
    raise ValueError("Faltan variables de conexión en el archivo .env")

# ---------------------------------------------------------
# Stored Procedure
# ---------------------------------------------------------
STORED_PROC = "GetValidSKUsByCatalogIDs"

# ---------------------------------------------------------
# Conexión a SQL Server
# ---------------------------------------------------------
def get_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};UID={username};PWD={password}"
    )
    try:
        conn = pyodbc.connect(conn_str)
        print(f"Conectado a SQL Server ({server} - {database})")
        return conn
    except Exception as e:
        print("Error al conectar con la base de datos")
        raise e

# ---------------------------------------------------------
# Lectura opcional de SKUs forzados
# ---------------------------------------------------------
def load_forced_skus():
    raw = os.getenv("FORCED_SKUS")
    if not raw:
        return None
    skus = [s.strip() for s in raw.split(",") if s.strip()]
    return skus if skus else None

# ---------------------------------------------------------
# Ejecución del SP
# ---------------------------------------------------------
def fetch_products_from_db(provider_id: int, catalog_id: int, skus=None) -> pd.DataFrame:
    print(f"Ejecutando SP '{STORED_PROC}' para ProviderID={provider_id}, CatalogID={catalog_id}")

    query = f"""
        EXEC dbo.{STORED_PROC}
            @catalogIDs = ?,
            @vendorid = ?,
            @cantDevuelta = ?,
            @skus = ?
    """

    sku_param = ",".join(skus) if skus else None

    conn = get_connection()
    df = pd.read_sql(
        query,
        conn,
        params=(str(catalog_id), provider_id, 1000, sku_param)
    )
    conn.close()

    print(f"SP ejecutado correctamente. Registros obtenidos: {len(df)}")
    return df

# ---------------------------------------------------------
# Guardado automático en DataStorage
# ---------------------------------------------------------
def save_dataframe(df: pd.DataFrame):
    data_dir = BASE_DIR / "DataStorage"
    data_dir.mkdir(exist_ok=True)

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"ProductosPatagonia_{timestamp}.csv"
    file_path = data_dir / file_name

    df.to_csv(file_path, index=False, encoding="utf-8-sig")
    print(f"Archivo guardado correctamente en: {file_path}")

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def main():
    combined_df = pd.DataFrame()

    forced_skus = load_forced_skus()
    if forced_skus:
        print(f"Modo SKUs forzados activado. SKUs recibidos: {len(forced_skus)}")

    for pid in PROVIDER_ID_LIST:
        df = fetch_products_from_db(
            provider_id=pid,
            catalog_id=CATALOG_ID,
            skus=forced_skus
        )
        combined_df = pd.concat([combined_df, df], ignore_index=True)

    if not combined_df.empty:
        save_dataframe(combined_df)
    else:
        print("No se obtuvieron registros de la base de datos")

# ---------------------------------------------------------
# Entry point
# ---------------------------------------------------------
if __name__ == "__main__":
    main()





