import sys
import pandas as pd
from pathlib import Path

# ---------------------------------------------------------
# 📂 Forzar ctcVendorController al PYTHONPATH
# ---------------------------------------------------------
BASE_DIR = Path(__file__).resolve()
while BASE_DIR.name != "ctcVendorController":
    BASE_DIR = BASE_DIR.parent
    if BASE_DIR.parent == BASE_DIR:
        raise FileNotFoundError("❌ No se encontró la carpeta raíz 'ctcVendorController'.")

sys.path.insert(0, str(BASE_DIR))

# ---------------------------------------------------------
# 📂 Ubicación base del proyecto
# ---------------------------------------------------------
DATA_DIR = BASE_DIR / "DataStorage"

# ---------------------------------------------------------
# 🧠 Vendor Diggit (desde config)
# ---------------------------------------------------------
from config.icbc_vendors import ICBC_VENDORS

DIGGIT_VENDOR_KEY = "diggit"

if DIGGIT_VENDOR_KEY not in ICBC_VENDORS:
    raise KeyError("❌ No existe el vendor 'diggit' en icbc_vendors.py")

DIGGIT_VENDOR_ID = str(ICBC_VENDORS[DIGGIT_VENDOR_KEY]["vendor_id"])

# ---------------------------------------------------------
# 🔍 Buscar el último archivo Patagonia
# ---------------------------------------------------------
def get_latest_patagonia_file() -> Path:
    csv_files = sorted(
        [f for f in DATA_DIR.glob("ProductosPatagonia_*.csv")],
        key=lambda x: x.stat().st_mtime,
        reverse=True
    )

    if not csv_files:
        raise FileNotFoundError(
            "❌ No se encontró ningún archivo ProductosPatagonia_*.csv en DataStorage."
        )

    print(f"📄 Último archivo encontrado: {csv_files[0].name}")
    return csv_files[0]

# ---------------------------------------------------------
# ✂️ Recortar los primeros 5 caracteres del SKU Patagonia
# ---------------------------------------------------------
def extract_diggit_sku(raw_sku: str) -> str:
    """
    Obtiene el SKU usable en Diggit eliminando los primeros 5 caracteres.
    Ejemplo:
    - '00432ME-HC5632-15' → 'ME-HC5632-15'
    """
    if not isinstance(raw_sku, str):
        raw_sku = str(raw_sku)

    raw_sku = raw_sku.strip()

    if len(raw_sku) <= 5:
        return ""

    return raw_sku[5:]

# ---------------------------------------------------------
# 🔗 Crear URL de Diggit
# ---------------------------------------------------------
def build_diggit_link(sku_diggit: str) -> str:
    return f"https://tiendadiggit.com.ar/shop?search={sku_diggit}&order=name+asc"

# ---------------------------------------------------------
# 🛠 Construir dataset final
# ---------------------------------------------------------
def generate_diggit_link_dataset() -> pd.DataFrame:
    latest_csv = get_latest_patagonia_file()
    df = pd.read_csv(latest_csv, dtype=str)

    # Validar columnas necesarias
    required_columns = ["sku", "ProviderId"]
    for col in required_columns:
        if col not in df.columns:
            raise KeyError(f"❌ No se encontró la columna '{col}' en el archivo Patagonia.")

    # Filtrar solo productos Diggit
    df_diggit = df[df["ProviderId"] == DIGGIT_VENDOR_ID].copy()

    if df_diggit.empty:
        raise ValueError("❌ No se encontraron productos de Diggit en el archivo Patagonia.")

    print(f"🧩 Productos Diggit encontrados: {len(df_diggit)}")

    df_diggit["SKU_PATAGONIA"] = df_diggit["sku"].astype(str)

    # Crear SKU_DIGGIT eliminando los primeros 5 caracteres
    df_diggit["SKU_DIGGIT"] = df_diggit["SKU_PATAGONIA"].apply(extract_diggit_sku)

    # Crear link final
    df_diggit["URL_DIGGIT"] = df_diggit["SKU_DIGGIT"].apply(build_diggit_link)

    final_df = df_diggit[["SKU_PATAGONIA", "SKU_DIGGIT", "URL_DIGGIT"]].copy()

    print("✅ Dataset Diggit generado correctamente.")
    print(final_df.head())

    return final_df

# ---------------------------------------------------------
# ▶️ Ejecución directa
# ---------------------------------------------------------
if __name__ == "__main__":
    link_df = generate_diggit_link_dataset()

    output_file = DATA_DIR / "Diggit_Links.csv"
    link_df.to_csv(output_file, index=False, encoding="utf-8-sig")

    print(f"💾 Archivo generado: {output_file}")
