import sys
import time
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.exceptions import ConnectionError, Timeout, RequestException

# =========================================================
# Asegurar que la raíz ctcVendorController esté en sys.path
# =========================================================

BASE_DIR = Path(__file__).resolve()
while BASE_DIR.name != "ctcVendorController":
    BASE_DIR = BASE_DIR.parent
    if BASE_DIR.parent == BASE_DIR:
        raise FileNotFoundError("No se encontró la carpeta raíz 'ctcVendorController'.")

if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# =========================================================
# Imports del proyecto
# =========================================================

from config.icbc_vendors import ICBC_VENDORS

print("RUNNING ShowHTML_ICBC.py – FULL DATALAYER MODE (SAFE)")

# =========================
# CONFIG GENERAL
# =========================

BASE_URL_TEMPLATE = (
    "https://mall.icbc.com.ar/buscar"
    "?controller=search"
    "&orderby=outstanding"
    "&orderway=desc"
    "&search_query={vendor}"
)

PAGE_PARAM = "p"
START_PAGE = 1
SLEEP_SECONDS = 1
MAX_RETRIES = 3

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120 Safari/537.36"
    ),
    "Accept-Language": "es-AR,es;q=0.9",
}

# =========================
# PATHS
# =========================

DATA_STORAGE_DIR = BASE_DIR / "DataStorage"
DATA_STORAGE_DIR.mkdir(exist_ok=True)

# =========================
# DETECTAR VENDOR DESDE ProductosPatagonia
# =========================

def detect_vendor_from_productos_patagonia() -> str:
    files = list(DATA_STORAGE_DIR.glob("ProductosPatagonia_*.csv"))

    if not files:
        raise FileNotFoundError("❌ No se encontraron archivos ProductosPatagonia_*.csv")

    latest_file = max(files, key=lambda f: f.stat().st_mtime)
    print(f"🧠 Leyendo ProviderId desde: {latest_file.name}")

    df = pd.read_csv(latest_file)

    if "ProviderId" not in df.columns:
        raise ValueError("❌ La columna ProviderId no existe en ProductosPatagonia")

    provider_ids = df["ProviderId"].dropna().unique()

    if len(provider_ids) != 1:
        raise ValueError(
            f"❌ ProductosPatagonia contiene múltiples ProviderId: {provider_ids}"
        )

    provider_id = int(provider_ids[0])

    for vendor_key, cfg in ICBC_VENDORS.items():
        if cfg.get("vendor_id") == provider_id:
            print(f"🧩 ProviderId {provider_id} → vendor '{vendor_key}'")
            return vendor_key

    raise ValueError(f"❌ ProviderId {provider_id} no está configurado en ICBC_VENDORS")

# =========================
# URL BUILDER
# =========================

def build_url(base_url: str, page: int) -> str:
    parsed = urlparse(base_url)
    qs = parse_qs(parsed.query)
    qs[PAGE_PARAM] = [str(page)]

    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            urlencode(qs, doseq=True),
            ""
        )
    )

# =========================
# REQUEST SAFE GET
# =========================

def safe_get(session: requests.Session, url: str):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return session.get(url, timeout=30)
        except (ConnectionError, Timeout) as e:
            print(f"⚠️ Error de red (intento {attempt}/{MAX_RETRIES})")
            print(f"    {e}")
            time.sleep(2)

    raise ConnectionError("❌ Fallaron todos los intentos de conexión")

# =========================
# DETECTAR MAX PAGINAS
# =========================

def get_last_page_from_html(html: str) -> int:
    soup = BeautifulSoup(html, "html.parser")
    pagination = soup.select_one("ul.pagination")

    if not pagination:
        print("⚠️ No se encontró paginación, se asume 1 página")
        return 1

    page_numbers = []
    for span in pagination.select("span"):
        text = span.get_text(strip=True)
        if text.isdigit():
            page_numbers.append(int(text))

    if not page_numbers:
        print("⚠️ No se encontraron números de página, se asume 1")
        return 1

    last_page = max(page_numbers)
    print(f"📄 Total páginas detectadas: {last_page}")
    return last_page

# =========================
# MAIN
# =========================

def run():
    rows = []

    # -------------------------
    # Detectar vendor por ProviderId
    # -------------------------
    vendor_key = detect_vendor_from_productos_patagonia()
    print(f"▶ Vendor activo ICBC: {vendor_key}")

    BASE_URL = BASE_URL_TEMPLATE.format(vendor=vendor_key)

    session = requests.Session()
    session.headers.update(HEADERS)

    # -------------------------
    # Detectar límite real de páginas
    # -------------------------
    print("🔍 Detectando cantidad total de páginas…")
    first_url = build_url(BASE_URL, START_PAGE)

    try:
        first_resp = safe_get(session, first_url)
        first_resp.raise_for_status()
    except RequestException as e:
        print("❌ No se pudo acceder a ICBC en la primera página.")
        print(e)
        return

    LAST_PAGE = get_last_page_from_html(first_resp.text)

    print(f"▶ Descargando <script data-keepinline> ICBC ({vendor_key}) – páginas {START_PAGE} a {LAST_PAGE}")

    # -------------------------
    # Iteración principal
    # -------------------------
    for page in range(START_PAGE, LAST_PAGE + 1):
        url = build_url(BASE_URL, page)
        print(f"\n⏬ Página {page} → {url}")

        try:
            resp = safe_get(session, url)
            resp.raise_for_status()
        except RequestException as e:
            print("❌ No se pudo acceder a ICBC. Corte del proceso.")
            print(e)
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        script_tag = soup.find("script", attrs={"data-keepinline": "true"})
        script_content = script_tag.get_text(strip=False) if script_tag else ""

        rows.append({
            "vendor": vendor_key,
            "page": page,
            "url": url,
            "datalayer_script": script_content
        })

        print(f"   ✔ Script capturado ({len(script_content)} caracteres)")
        time.sleep(SLEEP_SECONDS)

    # -------------------------
    # Guardar CSV
    # -------------------------
    df = pd.DataFrame(rows, columns=["vendor", "page", "url", "datalayer_script"])

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output = DATA_STORAGE_DIR / f"{vendor_key}_ICBC_DataLayerRaw_{ts}.csv"

    df.to_csv(output, index=False, encoding="utf-8")

    print("\n=========================")
    print(f"✅ Archivo generado: {output}")
    print(f"📄 Total páginas procesadas: {len(df)}")
    print("=========================")

# =========================
# ENTRY POINT
# =========================

if __name__ == "__main__":
    run()