# src/storage.py
from __future__ import annotations
import pickle
from pathlib import Path
from typing import Any, Optional, Dict, List
from datetime import datetime, date

import pandas as pd
from .config import PICKLE_DIR, CSV_DIR, LOG_DIR, RETENTION_DAYS

# TZ local para el prefijo
try:
    from zoneinfo import ZoneInfo  # py>=3.9
    _TZ = ZoneInfo("America/Argentina/Buenos_Aires")
except Exception:
    _TZ = None

# ----------------- helpers de fecha/nombre -----------------
def _today_str() -> str:
    now = datetime.now(_TZ) if _TZ else datetime.now()
    return now.strftime("%Y-%m-%d")

def _normalize_basename(name: str) -> str:
    return Path(name).stem

def _with_ext(basename: str, ext: str) -> str:
    base = _normalize_basename(basename)
    if not ext.startswith("."):
        ext = "." + ext
    return base + ext

def _with_date_prefix(name_with_ext: str, date_str: Optional[str] = None) -> str:
    return f"{(date_str or _today_str())}_{name_with_ext}"

# Extrae fecha del prefijo YYYY-MM-DD_ del filename; devuelve date o None
def _extract_date_prefix(filename: str) -> Optional[date]:
    # espera "YYYY-MM-DD_..."
    try:
        prefix = filename.split("_", 1)[0]
        if len(prefix) == 10:
            y, m, d = prefix.split("-")
            return date(int(y), int(m), int(d))
    except Exception:
        pass
    return None

def _find_latest_file(base_dir: Path, pattern: str) -> Path:
    """
    Busca el archivo más reciente en base_dir según un patrón glob.
    Devuelve Path o lanza FileNotFoundError si no hay coincidencias.
    """
    files = sorted(base_dir.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No se encontraron archivos que coincidan con {pattern} en {base_dir}")
    return files[-1]


def _purge_dir_by_date(dirpath: Path, ext: str, keep_days: Optional[int]) -> List[Path]:
    """
    Borra archivos en dirpath con nombre que comience por 'YYYY-MM-DD_' y terminación 'ext'
    cuya fecha sea más antigua que 'keep_days'. Devuelve lista de Paths borrados.
    """
    deleted: List[Path] = []
    if not keep_days or keep_days <= 0:
        return deleted

    now_d = (datetime.now(_TZ) if _TZ else datetime.now()).date()
    dirpath.mkdir(parents=True, exist_ok=True)
    for p in dirpath.glob(f"*{ext}"):
        if not p.is_file():
            continue
        dt = _extract_date_prefix(p.name)
        if not dt:
            continue  # nombres sin prefijo: no tocamos
        age = (now_d - dt).days
        if age > keep_days:
            try:
                p.unlink()
                deleted.append(p)
            except Exception:
                # mejor no fallar por no poder borrar un archivo
                pass
    return deleted

# ----------------- Pickle -----------------
def save_pickle(obj: Any, basename: str, *, date_str: Optional[str] = None) -> Path:
    """
    Guarda en PICKLE_DIR con nombre: YYYY-MM-DD_<basename>.p y purga archivos viejos.
    """
    PICKLE_DIR.mkdir(parents=True, exist_ok=True)
    filename = _with_ext(basename, ".p")
    out = PICKLE_DIR / _with_date_prefix(filename, date_str)
    with out.open("wb") as f:
        pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)
    return out

def load_pickle(basename: str, *, date_str: Optional[str] = None, latest: bool = True) -> Any:
    filename = _with_ext(basename, ".p")
    if date_str:
        path = PICKLE_DIR / _with_date_prefix(filename, date_str)
        if not path.is_file():
            raise FileNotFoundError(f"No existe: {path}")
        with path.open("rb") as f:
            return pickle.load(f)
    matches = sorted(PICKLE_DIR.glob(f"*_{filename}"))
    if not matches:
        raise FileNotFoundError(f"No se encontró '*_{filename}' en {PICKLE_DIR}")
    path = matches[-1] if latest else matches[0]
    with path.open("rb") as f:
        return pickle.load(f)
    
def load_newest_pickle(
    base_dir: Path | str | None = None,
    pattern: str = "*.p",
):
    """
    Carga el pickle más reciente en base_dir según el patrón indicado.
    Si no se especifica base_dir, usa el directorio global PICKLE_DIR.
    """
    if base_dir is None:
        base_dir = PICKLE_DIR
    elif isinstance(base_dir, str):
        base_dir = Path(base_dir)
    latest = _find_latest_file(base_dir, pattern)
    with latest.open("rb") as f:
        return pickle.load(f)


# ----------------- CSV -----------------
def save_csv(
    df: pd.DataFrame,
    basename: str,
    *,
    index: bool = False,
    sep: str = ";",
    encoding: str = "utf-8",
    na_rep: str = "",
    decimal: str = ".",
    quoting: Optional[int] = None,
    date_str: Optional[str] = None,
) -> Path:
    """
    Guarda en CSV_DIR con nombre: YYYY-MM-DD_<basename>.csv y purga archivos viejos.
    """
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    filename = _with_ext(basename, ".csv")
    out = CSV_DIR / _with_date_prefix(filename, date_str)

    kwargs: Dict[str, Any] = dict(index=index, sep=sep, encoding=encoding, na_rep=na_rep, decimal=decimal)
    if quoting is not None:
        import csv
        kwargs["quoting"] = quoting

    df.to_csv(out, **kwargs)
    return out

def load_csv(
    basename: str,
    *,
    date_str: Optional[str] = None,
    latest: bool = True,
    sep: str = ";",
    encoding: str = "utf-8",
    decimal: str = ".",
    dtype: Optional[dict] = None,
) -> pd.DataFrame:
    filename = _with_ext(basename, ".csv")
    if date_str:
        path = CSV_DIR / _with_date_prefix(filename, date_str)
        if not path.is_file():
            raise FileNotFoundError(f"No existe: {path}")
        return pd.read_csv(path, sep=sep, encoding=encoding, decimal=decimal, dtype=dtype)
    matches = sorted(CSV_DIR.glob(f"*_{filename}"))
    if not matches:
        raise FileNotFoundError(f"No se encontró '*_{filename}' en {CSV_DIR}")
    path = matches[-1] if latest else matches[0]
    return pd.read_csv(path, sep=sep, encoding=encoding, decimal=decimal, dtype=dtype)


def load_newest_csv(
    base_dir: Path | str | None = None,
    pattern: str = "*.csv",
    *,
    sep: str = ";",
    encoding: str = "utf-8",
    decimal: str = ".",
    dtype: dict | None = None,
) -> pd.DataFrame:
    """
    Carga el CSV más reciente en base_dir según el patrón indicado.
    Si no se especifica base_dir, usa el directorio global CSV_DIR.
    """
    if base_dir is None:
        base_dir = CSV_DIR
    elif isinstance(base_dir, str):
        base_dir = Path(base_dir)
    latest = _find_latest_file(base_dir, pattern)
    return pd.read_csv(latest, sep=sep, encoding=encoding, decimal=decimal, dtype=dtype)

# ----------------- LOGS (.txt) -----------------
def write_log(
    basename: str,
    text: str,
    *,
    date_str: Optional[str] = None,
    append: bool = True,
    encoding: str = "utf-8",
    ensure_newline: bool = True,
) -> Path:
    """
    Escribe log en LOG_DIR como YYYY-MM-DD_<basename>.txt y purga logs viejos.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    filename = _with_ext(basename, ".txt")
    path = LOG_DIR / _with_date_prefix(filename, date_str)

    payload = text
    if ensure_newline and not payload.endswith("\n"):
        payload += "\n"

    mode = "a" if append else "w"
    with path.open(mode, encoding=encoding, newline="") as f:
        f.write(payload)
    return path

# --------- util opcional para purga manual (si la querés usar desde notebook) ---------
def purge_all_old():
    """
    Ejecuta purga manual en pickles, csv y logs. Devuelve dict con listas de archivos borrados.
    """
    _purge_dir_by_date(PICKLE_DIR, ".p", RETENTION_DAYS)
    _purge_dir_by_date(CSV_DIR, ".csv", RETENTION_DAYS)
    _purge_dir_by_date(LOG_DIR, ".txt", RETENTION_DAYS)
