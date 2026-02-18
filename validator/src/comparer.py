import pandas as pd
from typing import List, Tuple, Dict
from common.storage import save_csv

def compare_catalogs(
    df_db: pd.DataFrame,
    df_web: pd.DataFrame,
    *,
    key_col_db: str,
    key_col_web: str,
    scraper_name: str = "",
    columns_to_compare: list[tuple[str, str]] | None = None,
) -> dict:
    """
    Compara catálogos entre base de datos (df_db) y web (df_web).
    Devuelve dict con conteo de diferencias, faltantes y sobrantes.
    """

    summary = {"missing": 0, "extra": 0, "matched": 0, "diffs": 0}

    # --- Normalizar claves ---
    db_keys = set(df_db[key_col_db].astype(str))
    web_keys = set(df_web[key_col_web].astype(str))

    # --- Detectar faltantes y sobrantes ---
    missing = db_keys - web_keys
    extra = web_keys - db_keys
    summary["missing"] = len(missing)
    summary["extra"] = len(extra)
    if len(missing) > 0:
        df_missing = df_db[df_db[key_col_db].isin(missing)]
        save_csv(df_missing, f"{scraper_name}_missing")
    if len(extra) > 0:
        df_extra = df_db[df_db[key_col_db].isin(extra)]
        save_csv(df_extra, f"{scraper_name}_extra")

    # --- Merge para detectar diferencias ---
    common_keys = db_keys & web_keys
    if not common_keys:
        return summary  # no hay nada en común
    summary["matched"] = len(common_keys)

    merged = pd.merge(
        df_db, df_web,
        how="inner",
        left_on=key_col_db,
        right_on=key_col_web,
        suffixes=("_db", "_web")
    )

    # --- Comparar columnas ---
    if columns_to_compare:
        diffs = 0
        for col_db, col_web in columns_to_compare:
            if col_db not in merged.columns or col_web not in merged.columns:
                continue
            mask = merged[f"{col_db}_db"].astype(str) != merged[f"{col_web}_web"].astype(str)
            diffs += mask.sum()
        summary["diffs"] = diffs
    else:
        summary["diffs"] = 0

    return summary
