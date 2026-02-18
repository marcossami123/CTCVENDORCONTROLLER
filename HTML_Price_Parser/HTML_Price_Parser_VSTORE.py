import re
import pandas as pd
from pathlib import Path
from datetime import datetime


# ==================================================
# PATHS
# ==================================================
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "DataStorage"


# ==================================================
# FILE HANDLING
# ==================================================
def get_latest_html_file(folder: Path):
    files = list(folder.glob("Vstore_HTML_*.csv"))
    if not files:
        return None
    return max(files, key=lambda f: f.stat().st_mtime)


# ==================================================
# PRICE PARSER (NUMÉRICO)
# ==================================================
def extract_price_from_html(html: str):
    """
    Devuelve PRECIO NUMÉRICO EN PESOS (int)
    Ej: 87549
    """
    if not isinstance(html, str) or not html.strip():
        return None

    # sellingPrice.highPrice (precio final)
    m = re.search(
        r'"sellingPrice"\s*:\s*\{[^}]*?"highPrice"\s*:\s*(\d+)',
        html
    )
    if m:
        return int(m.group(1))

    # fallback
    m = re.search(r'"highPrice"\s*:\s*(\d+)', html)
    if m:
        return int(m.group(1))

    return None


# ==================================================
# MAIN
# ==================================================
def main():
    latest_file = get_latest_html_file(DATA_DIR)
    if not latest_file:
        print("❌ No se encontró Vstore_HTML_*.csv")
        return

    print(f"📄 Procesando: {latest_file.name}")

    df = pd.read_csv(latest_file)

    df["PRICE_VSTORE"] = df["HTML"].apply(extract_price_from_html)

    # OUTPUT MINIMAL – NUMÉRICO
    output_df = df[[
        "SKU_PATAGONIA",
        "SKU_VSTORE",
        "PRICE_VSTORE"
    ]]

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = DATA_DIR / f"Vstore_Precios_{timestamp}.csv"

    output_df.to_csv(output_file, index=False, encoding="utf-8-sig")

    print(f"✅ Archivo generado: {output_file.name}")


if __name__ == "__main__":
    main()
