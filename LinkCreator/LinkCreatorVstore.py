import sys
import pandas as pd
from pathlib import Path

# ---------------------------------------------------------
# 📂 Forzar ctcVendorController al PYTHONPATH
# ---------------------------------------------------------
BASE_DIR = Path(__file__).resolve()
while BASE_DIR.name != "CTCVENDORCONTROLLER":
    BASE_DIR = BASE_DIR.parent
    if BASE_DIR.parent == BASE_DIR:
        raise FileNotFoundError("❌ No se encontró la carpeta raíz 'CTCVENDORCONTROLLER'.")

sys.path.insert(0, str(BASE_DIR))

# ---------------------------------------------------------
# 📂 Ubicación base del proyecto
# ---------------------------------------------------------
DATA_DIR = BASE_DIR / "DataStorage"

# ---------------------------------------------------------
# 🧠 Vendor Vstore (desde config)
# ---------------------------------------------------------
from config.icbc_vendors import ICBC_VENDORS

VSTORE_VENDOR_KEY = "vstore"

if VSTORE_VENDOR_KEY not in ICBC_VENDORS:
    raise KeyError("❌ No existe el vendor 'vstore' en icbc_vendors.py")

VSTORE_VENDOR_ID = str(ICBC_VENDORS[VSTORE_VENDOR_KEY]["vendor_id"])

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
# ✂️ Recortar los primeros 5 caracteres (00405)
# ---------------------------------------------------------
def extract_vstore_sku(raw_sku: str) -> str:
    """
    Obtiene el SKU eliminando los primeros 5 caracteres.
    Ejemplo:
    - '00405SAVC20CCNMARF' → 'SAVC20CCNMARF'
    """
    if not isinstance(raw_sku, str):
        raw_sku = str(raw_sku)

    raw_sku = raw_sku.strip()

    if len(raw_sku) <= 5:
        return ""

    return raw_sku[5:]

# ---------------------------------------------------------
# 🔗 Crear URL de Vstore
# ---------------------------------------------------------
def build_vstore_link(sku_vstore: str) -> str:
    return f"https://www.vstore.com.ar/{sku_vstore}?_q={sku_vstore}&map=ft"

# ---------------------------------------------------------
# 🛠 Construir dataset final
# ---------------------------------------------------------
def generate_vstore_link_dataset() -> pd.DataFrame:
    latest_csv = get_latest_patagonia_file()
    df = pd.read_csv(latest_csv, dtype=str)

    # Validar columnas necesarias
    required_columns = ["sku", "ProviderId"]
    for col in required_columns:
        if col not in df.columns:
            raise KeyError(
                f"❌ No se encontró la columna '{col}' en el archivo Patagonia."
            )

    # Filtrar solo productos Vstore
    df_vstore = df[df["ProviderId"] == VSTORE_VENDOR_ID].copy()

    if df_vstore.empty:
        raise ValueError(
            "❌ No se encontraron productos de Vstore en el archivo Patagonia."
        )

    print(f"🧩 Productos Vstore encontrados: {len(df_vstore)}")

    df_vstore["SKU_PATAGONIA"] = df_vstore["sku"].astype(str)

    # Crear SKU_VSTORE eliminando los primeros 5 caracteres
    df_vstore["SKU_VSTORE"] = df_vstore["SKU_PATAGONIA"].apply(
        extract_vstore_sku
    )

    # Crear link final
    df_vstore["URL_VSTORE"] = df_vstore["SKU_VSTORE"].apply(
        build_vstore_link
    )

    final_df = df_vstore[
        ["SKU_PATAGONIA", "SKU_VSTORE", "URL_VSTORE"]
    ].copy()

    print("✅ Dataset Vstore generado correctamente.")
    print(final_df.head())

    return final_df

# ---------------------------------------------------------
# ▶️ Ejecución directa
# ---------------------------------------------------------
if __name__ == "__main__":
    link_df = generate_vstore_link_dataset()

    output_file = DATA_DIR / "Vstore_Links.csv"
    link_df.to_csv(output_file, index=False, encoding="utf-8-sig")

    print(f"💾 Archivo generado: {output_file}")

