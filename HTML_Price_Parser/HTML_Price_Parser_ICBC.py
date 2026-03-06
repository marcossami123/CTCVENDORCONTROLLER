import sys
import json
from pathlib import Path
from datetime import datetime

import pandas as pd

# =========================================================
# Asegurar que la raíz ctcVendorController esté en sys.path
# =========================================================

BASE_DIR = Path(__file__).resolve()
while BASE_DIR.name != "CTCVENDORCONTROLLER":
    BASE_DIR = BASE_DIR.parent
    if BASE_DIR.parent == BASE_DIR:
        raise FileNotFoundError("No se encontró la carpeta raíz 'CTCVENDORCONTROLLER'.")

if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# =========================================================
# Imports del proyecto
# =========================================================

from config.icbc_vendors import ICBC_VENDORS

print("RUNNING HTML_Price_Parser_ICBC.py")

# =========================================================
# PATHS
# =========================================================

DATA_STORAGE_DIR = BASE_DIR / "DataStorage"

# =========================================================
# UTIL: obtener último archivo DataLayer
# =========================================================

def get_latest_datalayer_file() -> Path:
    files = list(DATA_STORAGE_DIR.glob("*_ICBC_DataLayerRaw_*.csv"))
    if not files:
        raise FileNotFoundError("❌ No se encontraron archivos *_ICBC_DataLayerRaw_*.csv")

    latest = max(files, key=lambda f: f.stat().st_mtime)
    print(f"📄 DataLayer detectado: {latest.name}")
    return latest

# =========================================================
# UTIL: extraer objetos ecommerce.items desde script
# =========================================================

def extract_items_from_datalayer(script_text: str) -> list[dict]:
    """
    Extrae TODOS los objetos dentro de ecommerce.items
    usando parsing por balanceo de llaves (robusto).
    """
    if not script_text or "ecommerce" not in script_text:
        return []

    start = script_text.find('"items":')
    if start == -1:
        return []

    start = script_text.find("[", start)
    if start == -1:
        return []

    level = 0
    end = None
    for i in range(start, len(script_text)):
        if script_text[i] == "[":
            level += 1
        elif script_text[i] == "]":
            level -= 1
            if level == 0:
                end = i + 1
                break

    if end is None:
        return []

    items_block = script_text[start:end]

    try:
        return json.loads(items_block)
    except json.JSONDecodeError:
        return []

# =========================================================
# MAIN
# =========================================================

def run():
    datalayer_file = get_latest_datalayer_file()
    df_raw = pd.read_csv(datalayer_file)

    if "vendor" not in df_raw.columns:
        raise ValueError("❌ El DataLayer no contiene la columna 'vendor'")

    vendors = df_raw["vendor"].dropna().unique()
    if len(vendors) != 1:
        raise ValueError(f"❌ El DataLayer contiene múltiples vendors: {vendors}")

    vendor_key = vendors[0]

    if vendor_key not in ICBC_VENDORS:
        raise ValueError(f"❌ Vendor '{vendor_key}' no está en icbc_vendors.py")

    provider_id = ICBC_VENDORS[vendor_key]["vendor_id"]

    print(f"🧠 Vendor: {vendor_key} | ProviderId: {provider_id}")

    rows = []

    for _, row in df_raw.iterrows():
        script = str(row.get("datalayer_script", ""))

        items = extract_items_from_datalayer(script)

        for item in items:
            reference = item.get("reference")
            price = item.get("price")

            if not reference or price is None:
                continue

            rows.append({
                "ProviderId": provider_id,
                # 🔒 reference siempre como string
                "reference": str(reference).strip(),
                "price": float(price)
            })

    if not rows:
        print("⚠️ No se extrajeron pares reference/price")
        return

    df_out = pd.DataFrame(rows, columns=["ProviderId", "reference", "price"])

    # =====================================================
    # 🔒 FORZAR SKU (reference) A STRING (CRÍTICO)
    # =====================================================
    df_out["reference"] = df_out["reference"].astype(str).str.strip()

    # Eliminar duplicados exactos
    df_out = df_out.drop_duplicates()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output = DATA_STORAGE_DIR / f"{vendor_key}_ICBC_Parsed_Prices_{ts}.csv"

    df_out.to_csv(output, index=False, encoding="utf-8")

    print("\n=========================")
    print(f"✅ Archivo generado: {output}")
    print(f"📄 Total filas: {len(df_out)}")
    print(f"📄 Total referencias únicas: {df_out['reference'].nunique()}")
    print("=========================")

# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    run()