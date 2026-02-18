import sys
from pathlib import Path
from datetime import datetime

import pandas as pd

# =========================================================
# Asegurar raíz ctcVendorController en sys.path
# =========================================================

BASE_DIR = Path(__file__).resolve()
while BASE_DIR.name != "ctcVendorController":
    BASE_DIR = BASE_DIR.parent
    if BASE_DIR.parent == BASE_DIR:
        raise FileNotFoundError("No se encontró la carpeta raíz 'ctcVendorController'.")

if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

print("RUNNING PriceComparisonICBC.py")

# =========================================================
# Imports del proyecto
# =========================================================

from config.icbc_vendors import ICBC_VENDORS

# =========================================================
# PATHS
# =========================================================

DATA_STORAGE_DIR = BASE_DIR / "DataStorage"

# =========================================================
# UTILIDADES
# =========================================================

def get_latest_file(pattern: str) -> Path:
    files = list(DATA_STORAGE_DIR.glob(pattern))
    if not files:
        raise FileNotFoundError(f"❌ No se encontraron archivos {pattern}")
    return max(files, key=lambda f: f.stat().st_mtime)


def normalize_patagonia_sku(s: pd.Series) -> pd.Series:
    """
    Regla pedida:
    - sin ceros a la izquierda
    - sin los primeros 3 dígitos post-ceros
    """
    x = (
        s.astype(str)
        .str.strip()
        .str.lstrip("0")     # 1) sacar ceros a la izquierda
        .str[3:]             # 2) sacar primeros 3 dígitos
        .str.lstrip("0")     # 3) por si quedaron ceros otra vez
    )
    return x


def normalize_icbc_reference(s: pd.Series) -> pd.Series:
    """
    Normalización simple del lado ICBC.
    """
    return (
        s.astype(str)
        .str.strip()
        .str.lstrip("0")
    )

# =========================================================
# MAIN
# =========================================================

def run():
    # -------------------------
    # Archivos de entrada
    # -------------------------
    productos_file = get_latest_file("ProductosPatagonia_*.csv")
    icbc_prices_file = get_latest_file("*_ICBC_Parsed_Prices_*.csv")

    print(f"📄 ProductosPatagonia : {productos_file.name}")
    print(f"📄 ICBC Parsed Prices : {icbc_prices_file.name}")

    df_prod = pd.read_csv(productos_file)
    df_icbc = pd.read_csv(icbc_prices_file)

    # -------------------------
    # Detectar vendor ICBC
    # -------------------------
    if "ProviderId" not in df_prod.columns:
        raise ValueError("❌ ProductosPatagonia no contiene columna ProviderId")

    provider_ids = df_prod["ProviderId"].dropna().unique()
    if len(provider_ids) != 1:
        raise ValueError(f"❌ ProductosPatagonia contiene múltiples ProviderId: {provider_ids}")

    provider_id = int(provider_ids[0])

    vendor_key = None
    for k, cfg in ICBC_VENDORS.items():
        if cfg.get("vendor_id") == provider_id:
            vendor_key = k
            break

    if not vendor_key:
        raise ValueError(f"❌ ProviderId {provider_id} no está en icbc_vendors.py")

    print(f"🧠 Vendor ICBC: {vendor_key} | ProviderId: {provider_id}")

    # -------------------------
    # Validaciones columnas
    # -------------------------
    required_prod_cols = {"sku", "PrecioVenta"}
    required_icbc_cols = {"reference", "price"}

    if not required_prod_cols.issubset(df_prod.columns):
        raise ValueError(f"❌ ProductosPatagonia debe contener columnas {required_prod_cols}")

    if not required_icbc_cols.issubset(df_icbc.columns):
        raise ValueError(f"❌ ICBC Parsed Prices debe contener columnas {required_icbc_cols}")

    # -------------------------
    # 🔒 Normalización SKUs (según tu regla)
    # -------------------------
    df_prod["SKU_LIMPIO"] = normalize_patagonia_sku(df_prod["sku"])
    df_icbc["SKU_LIMPIO"] = normalize_icbc_reference(df_icbc["reference"])

    # (Opcional) Debug rápido: cuántos quedan vacíos
    empty_prod = (df_prod["SKU_LIMPIO"] == "") | (df_prod["SKU_LIMPIO"].isna())
    if empty_prod.any():
        print(f"⚠️ Patagonia: {int(empty_prod.sum())} SKUs quedaron vacíos tras normalización (revisar formatos)")

    # -------------------------
    # Merge (match real por SKU)
    # -------------------------
    df_merge = df_prod.merge(
        df_icbc,
        on="SKU_LIMPIO",
        how="inner"
    )

    if df_merge.empty:
        print("⚠️ No se encontraron SKUs coincidentes entre Patagonia e ICBC (con la regla actual).")
        return

    # Inyectar ProviderId post-merge
    df_merge["ProviderId"] = provider_id

    # -------------------------
    # ✅ Cálculos de auditoría
    # -------------------------
    df_merge["Precio_Patagonia"] = df_merge["PrecioVenta"].astype(float)
    df_merge["Precio_ICBC"] = df_merge["price"].astype(float)

    df_merge["Dif_Abs"] = df_merge["Precio_Patagonia"] - df_merge["Precio_ICBC"]

    denom = df_merge["Precio_ICBC"].replace(0, pd.NA)
    df_merge["Dif_%"] = (df_merge["Precio_Patagonia"] / denom) - 1
    df_merge["Dif_%"] = df_merge["Dif_%"].fillna(0)

    # -------------------------
    # Output final
    # -------------------------
    output_cols = [
        "ProviderId",
        "sku",
        "SKU_LIMPIO",
        "Precio_Patagonia",
        "Precio_ICBC",
        "Dif_Abs",
        "Dif_%"
    ]

    df_out = df_merge[output_cols].copy()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_path = DATA_STORAGE_DIR / f"Audit_ICBC_{vendor_key}_{ts}.csv"
    xlsx_path = DATA_STORAGE_DIR / f"Audit_ICBC_{vendor_key}_{ts}.xlsx"

    df_out.to_csv(csv_path, index=False, encoding="utf-8")
    df_out.to_excel(xlsx_path, index=False)

    print("\n=========================")
    print("✅ Audit ICBC generado")
    print(f"📄 CSV : {csv_path}")
    print(f"📄 XLSX: {xlsx_path}")
    print(f"📊 Productos auditados: {len(df_out)}")
    print("=========================")


if __name__ == "__main__":
    run()
