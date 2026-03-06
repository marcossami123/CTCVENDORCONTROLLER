import os
import sys
import pyodbc
import pandas as pd
import datetime
from pathlib import Path
from dotenv import load_dotenv

# ---------------------------------------------------------
# 🧭 Asegura que el módulo raíz 'ctcVendorController' esté disponible
# ---------------------------------------------------------
BASE_DIR = Path(__file__).resolve()
while BASE_DIR.name != "CTCVENDORCONTROLLER":
    BASE_DIR = BASE_DIR.parent
    if BASE_DIR.parent == BASE_DIR:
        raise FileNotFoundError("❌ No se encontró la carpeta raíz 'CTCVENDORCONTROLLER'.")

if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# ---------------------------------------------------------
# 🔐 Carga robusta de variables confidenciales desde .env
# 🔥 ESTO TIENE QUE IR ANTES DE IMPORTAR settings
# ---------------------------------------------------------
ENV_PATH = BASE_DIR / ".env"

if not ENV_PATH.exists():
    raise FileNotFoundError(f"❌ No se encontró el archivo .env en: {ENV_PATH}")

load_dotenv(dotenv_path=ENV_PATH)
print(f"✅ Archivo .env cargado desde: {ENV_PATH}")

# ---------------------------------------------------------
# 🔹 Importar configuración global (DESPUÉS del load_dotenv)
# ---------------------------------------------------------
from common.config import CATALOG_ID, settings

# Variables de conexión (ya validadas por Pydantic)
server = settings.DB_SERVER
database = settings.DB_NAME
username = settings.DB_USER
password = settings.DB_PASSWORD

if not all([server, database, username, password]):
    raise ValueError("❌ Faltan variables de conexión en el archivo .env")

# ---------------------------------------------------------
# 🧩 Stored Procedure a ejecutar
# ---------------------------------------------------------
STORED_PROC = "GetValidSKUsByCatalogIDs"

# ProviderID de Radio Sapienza
SAPIENZA_PROVIDER_ID = 450

# ---------------------------------------------------------
# 🔌 Conexión a SQL Server
# ---------------------------------------------------------
def get_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};UID={username};PWD={password}"
    )
    try:
        conn = pyodbc.connect(conn_str)
        print(f"✅ Conectado a SQL Server ({server} - {database})")
        return conn
    except Exception as e:
        print("❌ Error al conectar con la base de datos:")
        raise e

# ---------------------------------------------------------
# 🚀 Ejecutar el SP
# ---------------------------------------------------------
def fetch_sapienza_products(catalog_id: int) -> pd.DataFrame:
    print(
        f"\n🔍 Ejecutando SP '{STORED_PROC}' "
        f"para ProviderID={SAPIENZA_PROVIDER_ID}, CatalogID={catalog_id}..."
    )

    query = f"EXEC dbo.{STORED_PROC} @catalogIDs = ?, @vendorid = ?"

    conn = get_connection()
    df = pd.read_sql(query, conn, params=(str(catalog_id), SAPIENZA_PROVIDER_ID))
    conn.close()

    print(f"✅ SP '{STORED_PROC}' ejecutado correctamente.")
    print(f"📦 Registros obtenidos: {len(df)}\n")

    return df

# ---------------------------------------------------------
# 💾 Guardado automático
# ---------------------------------------------------------
def save_dataframe(df: pd.DataFrame):
    data_dir = BASE_DIR / "DataStorage"
    data_dir.mkdir(exist_ok=True)

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"ProductosPatagonia_{timestamp}.csv"
    file_path = data_dir / file_name

    df.to_csv(file_path, index=False, encoding="utf-8-sig")
    print(f"💾 Archivo guardado correctamente en:\n{file_path}\n")

# ---------------------------------------------------------
# ▶️ Ejecución directa
# ---------------------------------------------------------
if __name__ == "__main__":
    df = fetch_sapienza_products(CATALOG_ID)

    if not df.empty:
        save_dataframe(df)
    else:
        print("⚠️ No se obtuvieron registros de la base de datos.")

