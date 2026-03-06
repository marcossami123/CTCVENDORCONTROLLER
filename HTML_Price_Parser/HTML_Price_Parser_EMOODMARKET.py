import re
import pandas as pd
from pathlib import Path
import datetime

# ---------------------------------------------------------
# 📂 Ubicación base del proyecto
# ---------------------------------------------------------
BASE_DIR = Path(__file__).resolve()
while BASE_DIR.name != "CTCVENDORCONTROLLER":
    BASE_DIR = BASE_DIR.parent
    if BASE_DIR.parent == BASE_DIR:
        raise FileNotFoundError("❌ No se encontró la carpeta raíz 'CTCVENDORCONTROLLER'.")

DATA_DIR = BASE_DIR / "DataStorage"


# ---------------------------------------------------------
# 🔍 Buscar el CSV más reciente con HTML
# ---------------------------------------------------------
def get_latest_html_csv() -> Path:
    files = sorted(
        [f for f in DATA_DIR.glob("Emood_HTML_*.csv")],
        key=lambda x: x.stat().st_mtime,
        reverse=True
    )
    if not files:
        raise FileNotFoundError("❌ No se encontró ningún archivo Emood_HTML_*.csv en DataStorage.")
    print(f"📄 Último archivo HTML encontrado: {files[0].name}")
    return files[0]


# ---------------------------------------------------------
# 🔎 Extraer el valor de data-product-price del HTML
# ---------------------------------------------------------
def extract_price_from_html(html_text: str):
    """
    Busca data-product-price="XXXX" en el HTML.
    Devuelve el precio real en ARS como número flotante.
    """
    if not isinstance(html_text, str):
        return None

    # Buscar el patrón correcto
    pattern = r'data-product-price="(\d+)"'
    match = re.search(pattern, html_text)
    if not match:
        return None

    raw_value = match.group(1)

    try:
        value = int(raw_value)
        return value / 100  # dividir por 100 para obtener precio final
    except:
        return None


# ---------------------------------------------------------
# 🧠 Generar dataset con precios extraídos
# ---------------------------------------------------------
def generate_emood_price_dataset():
    html_csv_path = get_latest_html_csv()

    df = pd.read_csv(html_csv_path, dtype=str)

    required_cols = ["SKU_PATAGONIA", "SKU_EMOOD", "HTML"]
    for col in required_cols:
        if col not in df.columns:
            raise KeyError(f"❌ Falta la columna {col} en el CSV HTML.")

    print("\n🔍 Extrayendo precios desde el HTML...\n")

    prices = []

    for idx, html in enumerate(df["HTML"], start=1):
        price = extract_price_from_html(html)
        print(f"➡ ({idx}/{len(df)}) Precio encontrado: {price}")
        prices.append(price)

    df_prices = df[["SKU_PATAGONIA", "SKU_EMOOD"]].copy()
    df_prices["PRICE_EMOOD"] = prices

    print("\n✅ Precios extraídos correctamente.\n")

    return df_prices


# ---------------------------------------------------------
# 💾 Guardar resultado final
# ---------------------------------------------------------
def save_price_dataset(df: pd.DataFrame):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = DATA_DIR / f"Emood_Precios_{timestamp}.csv"
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"💾 Archivo guardado en:\n{output_path}\n")


# ---------------------------------------------------------
# ▶️ Ejecución directa
# ---------------------------------------------------------
if __name__ == "__main__":
    df_final = generate_emood_price_dataset()
    save_price_dataset(df_final)
