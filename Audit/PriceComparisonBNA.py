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

print("RUNNING PriceComparisonBNA.py")

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
    - quitar ceros a la izquierda
    - quitar los primeros 3 dígitos post-ceros
    - volver a quitar ceros por seguridad
    """
    return (
        s.astype(str)
        .str.strip()
        .str.lstrip("0")
        .str[3:]
        .str.lstrip("0")
    )


def normalize_bna_sku(s: pd.Series) -> pd.Series:
    """
    Normalización BNA:
    - string
    - quitar espacios
    - quitar ceros a la izquierda
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
    bna_prices_file = get_latest_file("BNA_Prices_*_*.csv")

    print(f"📄 ProductosPatagonia: {productos_file.name}")
    print(f"📄 BNA Prices        : {bna_prices_file.name}")

    df_prod = pd.read_csv(productos_file)
    df_bna = pd.read_csv(bna_prices_file)

    # -------------------------
    # Validaciones ProviderId (solo Patagonia)
    # -------------------------
    if "ProviderId" not in df_prod.columns:
        raise ValueError("❌ ProductosPatagonia no contiene columna ProviderId")

    provider_ids = df_prod["ProviderId"].dropna().unique()
    if len(provider_ids) != 1:
        raise ValueError(
            f"❌ ProductosPatagonia contiene múltiples ProviderId: {provider_ids}"
        )

    provider_id = int(provider_ids[0])
    print(f"🧠 ProviderId validado: {provider_id}")

    # -------------------------
    # Validaciones columnas
    # -------------------------
    required_prod_cols = {"sku", "PrecioVenta"}
    required_bna_cols = {"SKU", "PRICE"}

    if not required_prod_cols.issubset(df_prod.columns):
        raise ValueError(f"❌ ProductosPatagonia debe contener columnas {required_prod_cols}")

    if not required_bna_cols.issubset(df_bna.columns):
        raise ValueError(f"❌ BNA Prices debe contener columnas {required_bna_cols}")

    # -------------------------
    # 🔒 Normalización SKUs (MISMA LÓGICA QUE ICBC)
    # -------------------------
    df_prod["SKU_LIMPIO"] = normalize_patagonia_sku(df_prod["sku"])
    df_bna["SKU_LIMPIO"] = normalize_bna_sku(df_bna["SKU"])

    # Debug mínimo (opcional pero útil)
    empty_prod = df_prod["SKU_LIMPIO"].isna() | (df_prod["SKU_LIMPIO"] == "")
    if empty_prod.any():
        print(f"⚠️ Patagonia: {int(empty_prod.sum())} SKUs quedaron vacíos tras normalización")

    # -------------------------
    # Merge (MATCH REAL POR SKU)
    # -------------------------
    df_merge = df_prod.merge(
        df_bna,
        on="SKU_LIMPIO",
        how="inner"
    )

    if df_merge.empty:
        print("⚠️ No se encontraron SKUs coincidentes entre Patagonia y BNA (con la regla actual)")
        return

    # Inyectar ProviderId post-merge
    df_merge["ProviderId"] = provider_id

    # -------------------------
    # ✅ Cálculos de auditoría
    # -------------------------
    df_merge["Precio_Patagonia"] = df_merge["PrecioVenta"].astype(float)
    df_merge["Precio_BNA"] = df_merge["PRICE"].astype(float)

    df_merge["Dif_Abs"] = df_merge["Precio_Patagonia"] - df_merge["Precio_BNA"]

    denom = df_merge["Precio_BNA"].replace(0, pd.NA)
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
        "Precio_BNA",
        "Dif_Abs",
        "Dif_%"
    ]

    df_out = df_merge[output_cols].copy()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_path = DATA_STORAGE_DIR / f"Audit_BNA_{provider_id}_{ts}.csv"
    xlsx_path = DATA_STORAGE_DIR / f"Audit_BNA_{provider_id}_{ts}.xlsx"

    df_out.to_csv(csv_path, index=False, encoding="utf-8")
    df_out.to_excel(xlsx_path, index=False)

    print("\n=========================")
    print("✅ Audit BNA generado")
    print(f"📄 CSV : {csv_path}")
    print(f"📄 XLSX: {xlsx_path}")
    print(f"📊 Productos auditados: {len(df_out)}")
    print("=========================")

# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    run()