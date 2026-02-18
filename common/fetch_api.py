# common/fetch_api.py
import requests
import time
from datetime import datetime
from typing import Any, Dict, Optional

from .config import USER_AGENT, TIMEOUT_S, RETRIES, LOG_ALL, LOG_FETCH
from .storage import write_log

try:
    from zoneinfo import ZoneInfo
    _TZ = ZoneInfo("America/Argentina/Buenos_Aires")
except Exception:
    _TZ = None

def ts() -> str:
    now = datetime.now(_TZ) if _TZ else datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")

def get_json(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    retries: int = RETRIES,
    backoff_base: float = 1.5,
    verbose: bool = True,
) -> dict:
    """
    Realiza GET a una API JSON con reintentos exponenciales y logging.
    """
    headers = headers or {"User-Agent": USER_AGENT, "Accept": "application/json"}
    log_msg = f"{ts()} | GET {url}\n"
    last_err = None

    for attempt in range(1, retries + 1):
        try:
            if verbose:
                print(url)
                print(headers)
                print(params)
            resp = requests.get(url, headers=headers, params=params, timeout=TIMEOUT_S)
            resp.raise_for_status()
            data = resp.json()
            log_msg += f"{ts()} | OK intento {attempt} | status={resp.status_code}\n"
            if write_log and (LOG_FETCH or LOG_ALL):
                write_log("fetch_api", log_msg, append=True)
            return data
        except Exception as e:
            last_err = e
            log_msg += f"{ts()} | ERROR intento {attempt}/{retries} | {e}\n"
            if attempt < retries:
                time.sleep(backoff_base ** attempt)
            else:
                if write_log and (LOG_FETCH or LOG_ALL):
                    write_log("fetch_api", log_msg, append=True)
                raise

def post_json(
    url: str,
    payload: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
    retries: int = RETRIES,
    backoff_base: float = 1.5,
    verbose: bool = True,
) -> dict:
    """
    Realiza POST a una API JSON con payload, reintentos y logging.
    """
    headers = headers or {"User-Agent": USER_AGENT, "Content-Type": "application/json"}
    log_msg = f"{ts()} | POST {url}\n"
    last_err = None

    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=TIMEOUT_S)
            resp.raise_for_status()
            data = resp.json()
            log_msg += f"{ts()} | OK intento {attempt} | status={resp.status_code}\n"
            if write_log and (LOG_FETCH or LOG_ALL):
                write_log("fetch_api", log_msg, append=True)
            return data
        except Exception as e:
            last_err = e
            log_msg += f"{ts()} | ERROR intento {attempt}/{retries} | {e}\n"
            if attempt < retries:
                time.sleep(backoff_base ** attempt)
            else:
                if write_log and (LOG_FETCH or LOG_ALL):
                    write_log("fetch_api", log_msg, append=True)
                raise
