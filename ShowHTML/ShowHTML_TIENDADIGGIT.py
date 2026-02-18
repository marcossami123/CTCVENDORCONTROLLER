import pandas as pd
import requests
from pathlib import Path
import datetime
from time import sleep

# ---------------------------------------------------------
# 📂 Ubicación base del proyecto
# ---------------------------------------------------------
BASE_DIR = Path(__file__).resolve()
while BASE_DIR.name != "ctcVendorController":
    BASE_DIR = BASE_DIR.parent
    if BASE_DIR.parent == BASE_DIR:
        raise FileNotFoundError("❌ No se encontró la carpeta raíz 'ctcVendorController'.")

DATA_DIR = BASE_DIR / "DataStorage"

# ---------------------------------------------------------
# 🔍 Obtener el CSV con los links de Diggit
# ---------------------------------------------------------
def get_latest_diggit_links_csv() -> Path:
    files = sorted(
        [f for f in DATA_DIR.glob("Diggit_Links*.csv")],
        key=lambda x: x.stat().st_mtime,
        reverse=True
    )

    if not files:
        raise FileNotFoundError("❌ No se encontró ningún archivo Diggit_Links*.csv")

    print(f"📄 Último archivo encontrado: {files[0].name}")
    return files[0]

# ---------------------------------------------------------
# 🌐 Descargar HTML completo de un link
# ---------------------------------------------------------
def fetch_html(url: str) -> str:
    """
    Descarga el HTML completo de la página dada.
    Devuelve una cadena con el HTML, o un mensaje de error.
    """
    if not url or not isinstance(url, str):
        return "URL vacía"

    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0 Safari/537.36"
            )
        }

        resp = requests.get(url, headers=headers, timeout=10)

        if resp.status_code != 200:
            return f"ERROR_HTTP_{resp.status_code}"

        return resp.text  # HTML completo

    except Exception as e:
        return f"ERROR: {e}"

# ---------------------------------------------------------
# 🧠 Procesar todos los links y agregar HTML al dataset
# ---------------------------------------------------------
def generate_diggit_html_dataset() -> pd.DataFrame:
    csv_path = get_latest_diggit_links_csv()
    df = pd.read_csv(csv_path, dtype=str)

    if "URL_DIGGIT" not in df.columns:
        raise KeyError("❌ La columna URL_DIGGIT no existe en el archivo de links Diggit.")

    html_list = []

    print("\n🔍 Iniciando descarga de HTML para cada link...\n")

    for idx, url in enumerate(df["URL_DIGGIT"], start=1):
        print(f"➡ ({idx}/{len(df)}) Obteniendo HTML de: {url}")

        html = fetch_html(url)
        html_list.append(html)

        sleep(0.3)  # evita rate-limits / bans

    df["HTML"] = html_list

    print("\n✅ HTML agregado correctamente a cada fila.\n")

    return df

# ---------------------------------------------------------
# 💾 Guardar dataset final
# ---------------------------------------------------------
def save_html_dataset(df: pd.DataFrame):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = DATA_DIR / f"Diggit_HTML_{timestamp}.csv"

    df.to_csv(output_file, index=False, encoding="utf-8-sig")

    print(f"💾 Archivo guardado en:\n{output_file}\n")

# ---------------------------------------------------------
# ▶️ Ejecución directa
# ---------------------------------------------------------
if __name__ == "__main__":
    df_html = generate_diggit_html_dataset()
    save_html_dataset(df_html)
