from __future__ import annotations
import time
from datetime import datetime
import pandas as pd
from common.config import (
    COMPARE_MAPPINGS,
    PICKLE_DIR,
    SCRAPER_CATALOG_MAPPINGS,
    LOG_ALL,
    LOG_PIPELINE,
    PURGE_OLD_FILES,
    CSV_DIR,
    SCRAPER_KEY_COLUMNS
)
from common.storage import write_log, save_csv, purge_all_old, load_newest_csv, load_newest_pickle
from .comparer import compare_catalogs

# Zona horaria local opcional
try:
    from zoneinfo import ZoneInfo
    _TZ = ZoneInfo("America/Argentina/Buenos_Aires")
except Exception:
    _TZ = None


def _ts() -> str:
    now = datetime.now(_TZ) if _TZ else datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")


def _today_str() -> str:
    now = datetime.now(_TZ) if _TZ else datetime.now()
    return now.strftime("%Y-%m-%d")


def _validate_mapping(df_db: pd.DataFrame, df_web: pd.DataFrame, pairs: list[tuple[str, str]]):
    ok = True
    for col_db, col_web in pairs:
        if col_db not in df_db.columns:
            print(f"⚠️ Columna '{col_db}' no existe en df_db")
            ok = False
        if col_web not in df_web.columns:
            print(f"⚠️ Columna '{col_web}' no existe en df_web")
            ok = False
    return ok


def run_pipeline_for_channel(scraper_name: str) -> dict:
    """
    Ejecuta la validación para un scraper específico:
      1️⃣ Carga el CSV más reciente del scraper.
      2️⃣ Carga el pickle más reciente con los datos de la DB.
      3️⃣ Filtra df_db por los catálogos asociados.
      4️⃣ Compara ambos datasets (coincidencia, faltantes, diferencias).
    """

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_msg = f"{ts} | Inicia run_pipeline_for_channel | scraper={scraper_name}\n"
    summary = {"scraper": scraper_name, "missing": 0, "extra": 0, "diffs": 0, "status": "OK"}

    try:
        # =====================================================
        # 1️⃣ Cargar CSV más reciente del scraper
        # =====================================================
        pattern_csv = f"*{scraper_name}*.csv"  # ej: *ICBC*.csv
        df_web = load_newest_csv(CSV_DIR, pattern_csv)
        log_msg += f"{ts} | CSV cargado OK | filas={len(df_web)} | archivo patrón={pattern_csv}\n"

        # =====================================================
        # 2️⃣ Cargar pickle más reciente de la DB
        # =====================================================
        df_db = load_newest_pickle(PICKLE_DIR, pattern="*_products_expected.p")
        log_msg += f"{ts} | Pickle DB cargado OK | filas={len(df_db)}\n"

        # =====================================================
        # 3️⃣ Filtrar por CatalogIds asociados al scraper
        # =====================================================
        catalog_ids = SCRAPER_CATALOG_MAPPINGS.get(scraper_name, [])
        if catalog_ids and "CatalogoId" in df_db.columns:
            df_db = df_db[df_db["CatalogoId"].isin(catalog_ids)]
            log_msg += f"{ts} | Filtrado DB por CatalogIds={catalog_ids} | filas restantes={len(df_db)}\n"

        # =====================================================
        # 4️⃣ Detectar columnas clave para comparación
        # =====================================================
        key_web, key_db = SCRAPER_KEY_COLUMNS.get(scraper_name, ("sku", "sku"))

        # Fallback automático si no existen
        if key_web not in df_web.columns:
            key_web = next((c for c in df_web.columns if "ref" in c.lower() or "sku" in c.lower()), None)
        if key_db not in df_db.columns:
            key_db = next((c for c in df_db.columns if "ref" in c.lower() or "cod" in c.lower()), None)

        if not key_web or not key_db:
            raise KeyError(f"No se encontraron columnas clave para {scraper_name}. "
                           f"Web cols={list(df_web.columns)}, DB cols={list(df_db.columns)}")

        log_msg += f"{ts} | Columnas clave detectadas: web='{key_web}' db='{key_db}'\n"

        # =====================================================
        # 5️⃣ Comparar catálogos
        # =====================================================
        result = compare_catalogs(df_db, df_web, key_col_db=key_db, key_col_web=key_web, scraper_name=scraper_name)

        summary.update(result)
        summary["status"] = "OK"
        log_msg += f"{ts} | Comparación finalizada OK | diffs={result.get('diffs', 0)}\n"

    except Exception as e:
        summary["status"] = f"ERROR: {e}"
        log_msg += f"{ts} | ERROR run_pipeline_for_channel | {e}\n"
        print(f"[ERROR] {e}")

    finally:
        if LOG_ALL or LOG_PIPELINE:
            write_log("validator_pipeline", log_msg, append=True)
        print("✅ Ejecución finalizada.")
        return summary

# =====================================================
# Ejecutor global para todos los scrapers configurados
# =====================================================

def run_full_pipeline(verbose: bool = True):
    """
    Ejecuta run_pipeline_for_channel() para cada scraper definido
    en SCRAPER_CATALOG_MAPPINGS.  Genera un log resumen global.
    """
    t0 = time.time()
    log_msg = f"{_ts()} | Inicia run_full_pipeline | scrapers={list(SCRAPER_CATALOG_MAPPINGS.keys())}\n"
    summary = []

    for scraper_name in SCRAPER_CATALOG_MAPPINGS.keys():
        try:
            result = run_pipeline_for_channel(scraper_name)
            counts = {
                "scraper": scraper_name,
                "missing": result["missing"],
                "extra": result["extra"],
                "matched": result["matched"],
                "diffs": result["diffs"],
                "status": "OK",
            }
            summary.append(counts)
            
            log_msg += (f"{_ts()} | {scraper_name} OK | "
                        f"missing={counts['missing']} | extra={counts['extra']} | matched={counts['matched']} | diffs={counts['diffs']}\n")
        except Exception as e:
            summary.append({"scraper": scraper_name, "missing": 0, "extra": 0, "matched":0, "diffs": 0, "status": f"ERROR: {e}"})
            log_msg += f"{_ts()} | {scraper_name} ERROR | {e}\n"

    elapsed = time.time() - t0
    log_msg += f"{_ts()} | Finaliza run_full_pipeline | t={elapsed:.2f}s\n"

    if LOG_PIPELINE or LOG_ALL:
        write_log("validator_pipeline", log_msg, append=True)

    if verbose:
        print("===== Resumen global =====")
        for r in summary:
            print(f"{r['scraper']:<20}  missing={r['missing']:<5} extra={r['extra']:<5} matched={r['matched']:<5} diffs={r['diffs']:<5}  {r['status']}")

    return summary