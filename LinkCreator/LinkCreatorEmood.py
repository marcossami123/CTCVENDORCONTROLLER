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
# 🧠 Vendor Emood (desde config)
# ---------------------------------------------------------
from config.icbc_vendors import ICBC_VENDORS

EMOOD_VENDOR_KEY = "emood"

if EMOOD_VENDOR_KEY not in ICBC_VENDORS:
    raise KeyError("❌ No existe el vendor 'emood' en icbc_vendors.py")

EMOOD_VENDOR_ID = str(ICBC_VENDORS[EMOOD_VENDOR_KEY]["vendor_id"])

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
# ✂️ Recortar los primeros 5 caracteres (00436)
# ---------------------------------------------------------
def extract_emood_sku(raw_sku: str) -> str:
    """
    Obtiene el SKU de Emood eliminando los primeros 5 caracteres.
    Ejemplo:
    - '00436ME-HC5632-15' → 'ME-HC5632-15'
    """
    if not isinstance(raw_sku, str):
        raw_sku = str(raw_sku)

    raw_sku = raw_sku.strip()

    if len(raw_sku) <= 5:
        return ""

    return raw_sku[5:]

# ---------------------------------------------------------
# 🔗 Crear URL de Emood
# ---------------------------------------------------------
def build_emood_link(sku_emood: str) -> str:
    return f"https://www.emoodmarket.com/search/?q={sku_emood}"

# ---------------------------------------------------------
# 🛠 Construir dataset final
# ---------------------------------------------------------
def generate_emood_link_dataset() -> pd.DataFrame:
    latest_csv = get_latest_patagonia_file()
    df = pd.read_csv(latest_csv, dtype=str)

    # Validar columnas necesarias
    required_columns = ["sku", "ProviderId"]
    for col in required_columns:
        if col not in df.columns:
            raise KeyError(f"❌ No se encontró la columna '{col}' en el archivo Patagonia.")

    # Filtrar solo productos Emood
    df_emood = df[df["ProviderId"] == EMOOD_VENDOR_ID].copy()

    if df_emood.empty:
        raise ValueError("❌ No se encontraron productos de Emood en el archivo Patagonia.")

    print(f"🧩 Productos Emood encontrados: {len(df_emood)}")

    df_emood["SKU_PATAGONIA"] = df_emood["sku"].astype(str)

    # Crear SKU_EMOOD eliminando los primeros 5 caracteres
    df_emood["SKU_EMOOD"] = df_emood["SKU_PATAGONIA"].apply(extract_emood_sku)

    # Crear link final
    df_emood["URL_EMOOD"] = df_emood["SKU_EMOOD"].apply(build_emood_link)

    final_df = df_emood[["SKU_PATAGONIA", "SKU_EMOOD", "URL_EMOOD"]].copy()

    print("✅ Dataset Emood generado correctamente.")
    print(final_df.head())

    return final_df

# ---------------------------------------------------------
# ▶️ Ejecución directa
# ---------------------------------------------------------
if __name__ == "__main__":
    link_df = generate_emood_link_dataset()

    output_file = DATA_DIR / "Emood_Links.csv"
    link_df.to_csv(output_file, index=False, encoding="utf-8-sig")

    print(f"💾 Archivo generado: {output_file}")


