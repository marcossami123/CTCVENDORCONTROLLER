import os
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from time import sleep

# ---------------------------------------------------------
# Endpoint API Megatone / Doofinder
# ---------------------------------------------------------
API_URL = "https://us1-search.doofinder.com/6/7d78864dfd68192d967ce98f7af00970/_search"

# ---------------------------------------------------------
# Headers (equivalentes a Postman)
# ---------------------------------------------------------
HEADERS = {
    "authority": "us1-search.doofinder.com",
    "method": "GET",
    "origin": "https://www.megatone.net",
    "referer": "https://www.megatone.net/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
    "accept": "application/json, text/plain, */*",
    "accept-language": "es-AR,es;q=0.9",
    "connection": "keep-alive",
}

# ---------------------------------------------------------
# CSV más reciente de ProductosPatagonia
# ---------------------------------------------------------
def get_latest_productos_csv(data_storage_path: Path) -> Path:
    csv_files = [
        f for f in data_storage_path.glob("*.csv")
        if "ProductosPatagonia" in f.name
    ]

    if not csv_files:
        raise FileNotFoundError(
            f"No se encontraron archivos 'ProductosPatagonia' en {data_storage_path}"
        )

    latest_file = max(csv_files, key=os.path.getmtime)
    print(f"Ultimo archivo encontrado: {latest_file.name}")
    return latest_file

# ---------------------------------------------------------
# Consulta de precio a la API
# ---------------------------------------------------------
def fetch_sale_price(sku: str) -> float | None:
    try:
        params = {"query": sku}
        response = requests.get(
            API_URL,
            headers=HEADERS,
            params=params,
            timeout=15
        )

        if response.status_code != 200:
            print(f"Error {response.status_code} para SKU {sku}")
            return None

        data = response.json()
        results = data.get("results", [])

        if not results:
            print(f"Sin resultados para SKU {sku}")
            return None

        product = next(
            (p for p in results if sku in p.get("title", "")),
            results[0]
        )

        return (
            product.get("sale_price")
            or product.get("best_price")
            or product.get("price")
        )

    except Exception as e:
        print(f"Error consultando SKU {sku}: {e}")
        return None

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def main():
    base_path = Path(__file__).resolve().parent.parent
    data_storage_path = base_path / "DataStorage"

    latest_csv = get_latest_productos_csv(data_storage_path)
    df_products = pd.read_csv(latest_csv)

    # Detectar columna SKU
    sku_col = next(
        (c for c in df_products.columns if c.strip().lower() == "sku"),
        None
    )

    if not sku_col:
        raise ValueError("El archivo debe tener una columna llamada 'sku'.")

    skus = df_products[sku_col].dropna().astype(str).unique().tolist()
    print(f"Consultando precios en la API de Megatone para {len(skus)} productos")

    results = []
    for i, sku in enumerate(skus, start=1):
        sku_megatone = sku[5:] if len(sku) > 5 else sku
        sale_price = fetch_sale_price(sku_megatone)

        results.append({
            "SKU_Patagonia": sku,
            "SKU_Megatone": sku_megatone,
            "Sale_Price": sale_price
        })

        print(f"[{i}/{len(skus)}] SKU {sku_megatone} - Precio: {sale_price}")
        sleep(0.3)

    df_prices = pd.DataFrame(results)

    output_file = data_storage_path / (
        f"megatone_prices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    df_prices.to_csv(output_file, index=False, encoding="utf-8-sig")

    print(f"Resultados guardados en: {output_file}")
    print(df_prices.head())

    return df_prices

# ---------------------------------------------------------
# Entry point
# ---------------------------------------------------------
if __name__ == "__main__":
    main()


















