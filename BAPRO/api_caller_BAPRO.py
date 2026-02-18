import requests
import pandas as pd
import time
from pathlib import Path
from datetime import datetime

# -----------------------------------
# CONFIG
# -----------------------------------
BASE_URL = "https://www.provinciacompras.com.ar/api/catalog_system/pub/products/search"
REQUEST_DELAY = 0.2

BASE_PATH = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_PATH.parent
STORAGE_PATH = PROJECT_ROOT / "DataStorage"

# -----------------------------------
# Buscar último ProductosPatagonia
# -----------------------------------
def get_latest_productos_patagonia():
    files = list(STORAGE_PATH.glob("ProductosPatagonia_*.csv"))

    if not files:
        raise FileNotFoundError("No se encontraron archivos ProductosPatagonia")

    latest_file = max(files, key=lambda f: f.stat().st_mtime)

    print(f"Archivo encontrado: {latest_file.name}")
    return latest_file


# -----------------------------------
# Consulta API Provincia
# -----------------------------------
def get_product(ref_id):

    params = {"fq": f"alternateIds_RefId:{ref_id}"}

    try:
        r = requests.get(BASE_URL, params=params, timeout=15)

        if r.status_code != 200:
            print(f"❌ Error HTTP para {ref_id}")
            return None

        data = r.json()

        if not data:
            print(f"⚠ No encontrado: {ref_id}")
            return None

        product = data[0]

        item = product["items"][0]
        seller = item["sellers"][0]
        offer = seller["commertialOffer"]

        price = offer.get("Price")

        return {
            "productReference": product.get("productReference"),
            "price": price
        }

    except Exception as e:
        print(f"❌ Error consultando {ref_id}: {e}")
        return None


# -----------------------------------
# MAIN
# -----------------------------------
def main():

    productos_file = get_latest_productos_patagonia()

    df = pd.read_csv(productos_file, dtype=str)

    if "sku" not in df.columns:
        raise ValueError("No existe columna sku")

    skus = df["sku"].dropna().astype(str).unique()

    print(f"Consultando {len(skus)} SKUs...")

    rows = []

    for i, sku in enumerate(skus, 1):

        # ✅ quitar primeros 5 caracteres
        api_sku = sku[5:]

        print(f"[{i}/{len(skus)}] {sku} → API: {api_sku}")

        result = get_product(api_sku)

        if result:
            rows.append({
                "SKU_original": sku,
                "SKU_API": api_sku,
                "productReference": result["productReference"],
                "Price": result["price"]
            })
        else:
            rows.append({
                "SKU_original": sku,
                "SKU_API": api_sku,
                "productReference": None,
                "Price": None
            })

        time.sleep(REQUEST_DELAY)

    output_df = pd.DataFrame(rows)

    # -------- nombre archivo final --------
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_name = f"PROVINCIA_Prices_{timestamp}.xlsx"

    output_path = STORAGE_PATH / output_name

    output_df.to_excel(output_path, index=False)

    print("\n✅ Archivo generado:")
    print(output_path)


# -----------------------------------
if __name__ == "__main__":
    main()