import sys
from pathlib import Path
from typing import Any, Dict, Optional, List
import requests
import csv
from datetime import datetime
import pandas as pd

# --------------------------------------------------
# 🔧 BOOTSTRAP PARA EJECUCIÓN DIRECTA
# --------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# --------------------------------------------------
# Imports de config
# --------------------------------------------------
from config.bna_api_config import BNA_API
from config.icbc_vendors import ICBC_VENDORS


# --------------------------------------------------
# Utils Patagonia
# --------------------------------------------------
def get_latest_productos_patagonia() -> Path:
    datastorage = PROJECT_ROOT / "DataStorage"

    files = sorted(
        datastorage.glob("ProductosPatagonia_*.csv"),
        key=lambda f: f.stat().st_mtime,
        reverse=True
    )

    if not files:
        raise FileNotFoundError("No se encontraron archivos ProductosPatagonia_*.csv")

    return files[0]


def get_provider_ids_from_patagonia(csv_path: Path) -> set[int]:
    df = pd.read_csv(csv_path, usecols=["ProviderId"])
    return set(df["ProviderId"].dropna().unique())


def resolve_bna_vendors_by_vendor_id(provider_ids: set[int]) -> Dict[str, Dict[str, Any]]:
    """
    Match CORRECTO:
    ProductosPatagonia.ProviderId == ICBC_VENDORS.vendor_id

    Devuelve:
    {
        vendor_key: {
            "vendorid_BNA": int,
            "vendor_id": int
        }
    }
    """
    resolved: Dict[str, Dict[str, Any]] = {}

    for vendor_key, cfg in ICBC_VENDORS.items():
        vendor_id = cfg.get("vendor_id")
        vendorid_BNA = cfg.get("vendorid_BNA")

        if vendor_id is None or vendorid_BNA is None:
            continue

        if vendor_id in provider_ids:
            resolved[vendor_key] = {
                "vendorid_BNA": vendorid_BNA,
                "vendor_id": vendor_id
            }

    return resolved


# --------------------------------------------------
# API Caller BNA (paginación hasta 404 Record not found)
# --------------------------------------------------
def fetch_products_bna(
    vendorid_BNA: int,
    timeout: int = 30,
    verbose: bool = True
) -> Dict[str, Any]:

    all_variants: List[Dict[str, Any]] = []
    page = 1

    while True:
        params = {
            "sh": vendorid_BNA,
            "p": page
        }

        if verbose:
            print(f"→ BNA API | sh={vendorid_BNA} | p={page}")

        response = requests.get(
            BNA_API["url"],
            headers=BNA_API["headers"],
            params=params,
            timeout=timeout
        )

        # 🛑 Corte limpio: no hay más páginas
        if response.status_code == 404:
            try:
                data = response.json()
            except Exception:
                data = {}

            if data.get("error") == "Record not found":
                print(f"   🛑 Página {page}: Record not found → fin de paginación")
                break

            # 404 inesperado
            response.raise_for_status()

        # Otros errores reales
        response.raise_for_status()

        data = response.json()
        variants = data.get("variants", [])

        print(f"   📦 Variants página {page}: {len(variants)}")

        if not variants:
            print(f"   🛑 Página {page} sin variants → fin de paginación")
            break

        all_variants.extend(variants)
        page += 1

    return {
        "variants": all_variants
    }


# --------------------------------------------------
# Extract SKU + PRICE + ProviderId
# --------------------------------------------------
def extract_sku_price(
    variants: List[Dict[str, Any]],
    provider_id: int
) -> List[Dict[str, Any]]:

    rows: List[Dict[str, Any]] = []

    for v in variants:
        sku = v.get("sku")
        product = v.get("product") or {}

        # Precio efectivo: sale_price -> regular_price
        price = product.get("sale_price") or product.get("regular_price")

        if sku is None or price is None:
            continue

        rows.append({
            "ProviderId": provider_id,
            "SKU": sku,
            "PRICE": float(price)
        })

    return rows


# --------------------------------------------------
# CSV writer
# --------------------------------------------------
def save_csv(rows: List[Dict[str, Any]], vendor_key: str) -> Path:
    datastorage = PROJECT_ROOT / "DataStorage"
    datastorage.mkdir(exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"BNA_Prices_{vendor_key}_{ts}.csv"
    filepath = datastorage / filename

    with filepath.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["ProviderId", "SKU", "PRICE"]
        )
        writer.writeheader()
        writer.writerows(rows)

    return filepath


# --------------------------------------------------
# 🧪 EJECUCIÓN PRINCIPAL
# --------------------------------------------------
if __name__ == "__main__":

    productos_path = get_latest_productos_patagonia()
    print(f"📦 Usando ProductosPatagonia: {productos_path.name}")

    provider_ids = get_provider_ids_from_patagonia(productos_path)
    print(f"🔎 ProviderId detectados: {provider_ids}")

    bna_vendors = resolve_bna_vendors_by_vendor_id(provider_ids)

    if not bna_vendors:
        print(
            "⚠️ Ningún ProviderId de ProductosPatagonia tiene vendorid_BNA configurado.\n"
            "ℹ️ No se realizará ninguna consulta a la API de BNA."
        )
        sys.exit(0)

    for vendor_key, vendor_data in bna_vendors.items():
        vendorid_BNA = vendor_data["vendorid_BNA"]
        provider_id = vendor_data["vendor_id"]

        print(
            f"\n🚀 Bajando BNA para vendor '{vendor_key}' "
            f"(ProviderId={provider_id} | sh={vendorid_BNA})"
        )

        data = fetch_products_bna(vendorid_BNA, verbose=True)
        variants = data.get("variants", [])

        print(f"📦 Total variants API (ALL PAGES): {len(variants)}")

        if not variants:
            print(f"⚠️ Sin productos para vendor '{vendor_key}'")
            continue

        rows = extract_sku_price(variants, provider_id)

        print(f"📊 SKU con precio efectivo: {len(rows)}")

        if not rows:
            print(f"⚠️ No se pudieron extraer precios para '{vendor_key}'")
            continue

        output = save_csv(rows, vendor_key)
        print(f"✅ CSV generado: {output}")
