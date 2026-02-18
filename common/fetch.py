import ssl
import certifi
import urllib.request
from urllib.error import HTTPError, URLError
import time
from datetime import datetime
import pandas as pd
import pyodbc
from .storage import load_newest_pickle
from .config import (
    USER_AGENT, TIMEOUT_S, RETRIES,
    LOG_ALL, LOG_FETCH,
    SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASS, SQL_DRIVER,
    PICKLE_DIR, SCRAPER_CATALOG_MAPPINGS
)

# Contexto SSL
_SSL_CTX = ssl.create_default_context(cafile=certifi.where())


# helper de timestamp local (AR)
try:
    from zoneinfo import ZoneInfo
    _TZ = ZoneInfo("America/Argentina/Buenos_Aires")
except Exception:
    _TZ = None

def ts() -> str:
    now = datetime.now(_TZ) if _TZ else datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")

# import diferido para evitar ciclos de import
try:
    from .storage import write_log
except Exception:
    write_log = None



def gets_html(url: str, timeout: float = TIMEOUT_S, retries: int = RETRIES, backoff_base: float = 1.2, verbose: bool = False) -> str:
    """
    GET con urllib y SSL confiable. Devuelve el HTML como str.
    Loguea en una variable de texto y escribe el .txt justo antes del return (o antes de relanzar error).
    """
    headers = {"User-Agent": USER_AGENT}

    # ── Mensaje acumulado de log ───────────────────────────────────────────────
    log_msg = f"{ts()} comienza funcion gets_html\n"

    last_err = None
    for attempt in range(1, retries + 1):
        # intento N
        log_msg += f"{ts()} obtencion del HTML - intento {attempt} | url={url}\n"
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, context=_SSL_CTX, timeout=timeout) as resp:
                charset = resp.headers.get_content_charset() or "utf-8"
                body = resp.read()
                html = body.decode(charset, errors="replace")

                if verbose:
                    status = getattr(resp, "status", "N/A")
                    print(f"[GET OK] {url} status={status} bytes={len(body)} charset={charset}")

                # finaliza OK
                log_msg += f"{ts()} Obtencion exitosa del HTML - finaliza funcion gets_html\n"
                if write_log and (LOG_FETCH or LOG_ALL):
                    write_log("fetch", log_msg, append=True)
                return html

        except (HTTPError, URLError, TimeoutError) as e:
            last_err = e
            if verbose:
                print(f"[GET ERR] attempt {attempt}/{retries} url={url} -> {e}")
            if attempt < retries:
                time.sleep(backoff_base ** attempt)
            # continúa al próximo intento

    # Si llegó acá, fallaron todos los intentos
    log_msg += f"{ts()} Fallo al obtener el HTML - finaliza funcion gets_html\n"
    if write_log and (LOG_FETCH or LOG_ALL):
        write_log("fetch", log_msg, append=True)
    # re-lanza el último error para que el caller decida
    raise last_err


# ────────────────────────────────────────────────
# DATABASE FETCH (SQL Server)
# ────────────────────────────────────────────────
def fetch_stored_procedure(
    sp_name: str,
    params: dict = None,
    timeout: float = TIMEOUT_S,
    verbose: bool = True
) -> pd.DataFrame:
    """
    Ejecuta un Stored Procedure con parámetros en SQL Server y devuelve un DataFrame.
    Mantiene estructura de logs y manejo de errores coherente con gets_html.
    """
    conn_str = (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};PWD={SQL_PASS};TrustServerCertificate=yes;"
    )

    log_msg = f"{ts()} comienza funcion fetch_stored_procedure | SP={sp_name}\n"
    start = time.time()

    try:
        with pyodbc.connect(conn_str, timeout=timeout) as conn:
            param_str = ", ".join(f"{k}=?" for k in params) if params else ""
            sql = f"EXEC {sp_name} {param_str}"
            values = tuple(params.values()) if params else ()
            df = pd.read_sql(sql, conn, params=values)
            log_msg += f"{ts()} OK SP={sp_name} | rows={len(df)}\n"
    except Exception as e:
        log_msg += f"{ts()} ERROR fetch_stored_procedure | {e}\n"
        if write_log and (LOG_FETCH or LOG_ALL):
            write_log("fetch", log_msg, append=True)
        raise

    elapsed = time.time() - start
    log_msg += f"{ts()} Finaliza fetch_stored_procedure | t={elapsed:.2f}s\n"
    if write_log and (LOG_FETCH or LOG_ALL):
        write_log("fetch", log_msg, append=True)

    if verbose:
        print(f"[SP OK] {sp_name} → {len(df)} filas (t={elapsed:.2f}s)")
    return df


# ────────────────────────────────────────────────
# LOAD EXPECTED PRODUCTS FROM PICKLE
# ────────────────────────────────────────────────



def load_expected_products(scraper_name: str) -> pd.DataFrame:
    """
    Carga el último pickle *_products_expected.p y filtra los productos
    correspondientes al scraper indicado, según SCRAPER_CATALOG_MAPPINGS.

    Args:
        scraper_name: nombre del scraper (clave en SCRAPER_CATALOG_MAPPINGS)

    Returns:
        pd.DataFrame filtrado por CatalogoId, o vacío si no hay coincidencias.
    """
    try:
        df = load_newest_pickle(PICKLE_DIR, pattern="*_products_expected.p")
    except FileNotFoundError:
        print("❌ No se encontró ningún pickle *_products_expected.p en outputs/pickles/")
        return pd.DataFrame()

    catalog_ids = SCRAPER_CATALOG_MAPPINGS.get(scraper_name)
    if not catalog_ids:
        print(f"⚠️ No hay CatalogoId configurados para '{scraper_name}' en SCRAPER_CATALOG_MAPPINGS.")
        return pd.DataFrame()

    if "CatalogoId" not in df.columns:
        raise ValueError("El DataFrame no contiene la columna 'CatalogoId' necesaria para filtrar.")

    filtered = df[df["CatalogoId"].isin(catalog_ids)].copy()
    print(f"✅ Productos esperados cargados: {len(filtered)} filas (de {len(df)}) para catálogos {catalog_ids}")
    return filtered
