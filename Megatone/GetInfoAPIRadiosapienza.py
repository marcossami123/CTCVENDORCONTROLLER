import sys
import os
import time
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------
# 📂 Forzar ctcVendorController al PYTHONPATH
# ---------------------------------------------------------
BASE_DIR = Path(__file__).resolve()
while BASE_DIR.name != "ctcVendorController":
    BASE_DIR = BASE_DIR.parent

sys.path.insert(0, str(BASE_DIR))

DATA_STORAGE_DIR = BASE_DIR / "DataStorage"

# ---------------------------------------------------------
# 🧠 Vendor Radio Sapienza (desde ICBC)
# ---------------------------------------------------------
from config.icbc_vendors import ICBC_VENDORS

SAPIENZA_VENDOR_KEY = "radio sapienza"

if SAPIENZA_VENDOR_KEY not in ICBC_VENDORS:
    raise KeyError("❌ No existe el vendor 'radio sapienza' en icbc_vendors.py")

SAPIENZA_VENDOR_ID = str(ICBC_VENDORS[SAPIENZA_VENDOR_KEY]["vendor_id"])

# ---------------------------------------------------------
# Config API / Output
# ---------------------------------------------------------
BASE_URL = "https://www.radiosapienza.com.ar/app/tienda"
HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_PREFIX = "radiosapienza_html"
OUTPUT_CSV = DATA_STORAGE_DIR / f"{OUTPUT_PREFIX}_{TIMESTAMP}.csv"

# ---------------------------------------------------------
# 🔍 Buscar último ProductosPatagonia
# ---------------------------------------------------------
def get_latest_patagonia_file() -> Path:
    files = sorted(
        DATA_STORAGE_DIR.glob("ProductosPatagonia_*.csv"),
        key=lambda x: x.stat().st_mtime,
        reverse=True
    )

    if not files:
        raise FileNotFoundError(
            "❌ No se encontraron archivos ProductosPatagonia_*.csv en DataStorage"
        )

    print(f"📄 Archivo Patagonia encontrado: {files[0].name}")
    return files[0]

# ---------------------------------------------------------
# ✂️ SKU Patagonia → SKU Vendor
# ---------------------------------------------------------
def sku_to_vendor(raw_sku: str) -> str:
    """
    Elimina los primeros 5 caracteres del SKU Patagonia
    """
    if raw_sku is None:
        return ""

    raw_sku = str(raw_sku).strip()

    if len(raw_sku) <= 5:
        return ""

    return raw_sku[5:]

# ---------------------------------------------------------
# 🌐 API CALL
# ---------------------------------------------------------
def fetch_html_from_api(sku_vendor: str) -> str | None:
    if not sku_vendor:
        return None

    params = {
        "idc": 0,
        "idf": 0,
        "ida": 0,
        "idt": 0,
        "col": "",
        "marcas": "",
        "articulos": "",
        "precios": "",
        "items": "",
        "buscador": sku_vendor,
        "orden": "",
        "deviceUsername": "",
        "deviceLogin": 0,
        "devicePais": "ar",
        "test": 0
    }

    try:
        response = requests.get(
            BASE_URL,
            params=params,
            headers=HEADERS,
            timeout=20
        )
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"[ERROR] API SKU {sku_vendor} → {e}")
        return None

# ---------------------------------------------------------
# ▶️ MAIN
# ---------------------------------------------------------
def main():

    # -----------------------------------------------------
    # 1️⃣ Productos Patagonia → solo Radio Sapienza
    # -----------------------------------------------------
    input_csv = get_latest_patagonia_file()
    df = pd.read_csv(input_csv, dtype=str)

    required_cols = ["sku", "ProviderId"]
    for col in required_cols:
        if col not in df.columns:
            raise KeyError(
                f"❌ No existe la columna '{col}' en {input_csv.name}"
            )

    df_sapienza = df[df["ProviderId"] == SAPIENZA_VENDOR_ID].copy()

    if df_sapienza.empty:
        raise ValueError(
            "❌ No se encontraron productos Radio Sapienza en ProductosPatagonia"
        )

    print(f"🧩 Productos Radio Sapienza encontrados: {len(df_sapienza)}")

    # -----------------------------------------------------
    # 2️⃣ Llamadas a la API
    # -----------------------------------------------------
    rows = []

    for _, row in df_sapienza.iterrows():
        sku_db = row["sku"]
        sku_vendor = sku_to_vendor(sku_db)

        print(f"Consultando API → SKU vendor {sku_vendor}")

        html = fetch_html_from_api(sku_vendor)

        rows.append({
            "SKUdb": sku_db,
            "SKUvendor": sku_vendor,
            "html": html
        })

        time.sleep(1)

    # -----------------------------------------------------
    # 3️⃣ Guardar CSV
    # -----------------------------------------------------
    pd.DataFrame(rows).to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"\n✔ CSV generado: {OUTPUT_CSV}")

# ---------------------------------------------------------
# ▶️ Entry point
# ---------------------------------------------------------
if __name__ == "__main__":
    main()

