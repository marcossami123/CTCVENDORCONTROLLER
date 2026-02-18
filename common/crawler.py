import urllib.parse
from typing import List
from .config import RATE_LIMIT_S

def _with_page_param(url: str, page: int) -> str:
    parsed = urllib.parse.urlparse(url)
    qs = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    qs["p"] = [str(page)]
    new_query = urllib.parse.urlencode(qs, doseq=True)
    return urllib.parse.urlunparse(parsed._replace(query=new_query))


def crawlerByPageNumber(
    seeds: List[str],
    *,
    start_page: int = 1,
    max_pages: int = 1,
    include_plain_seed: bool = False,
    verbose: bool = False,
) -> List[str]:
    """
    Devuelve la lista de URLs de páginas de listado generadas desde la semilla.
    No hace requests. Simplemente construye:
      seed?p=1, seed?p=2, ... seed?p=max_pages
    - Sin duplicados y preservando orden.
    - include_plain_seed=True agrega la semilla tal cual como primer URL.
    """
    urls: List[str] = []
    seen = set()
    for seed in seeds:
        if include_plain_seed:
            if seed not in seen:
                seen.add(seed)
                urls.append(seed)
                if verbose:
                    print(f"[ADD] {seed}")

        for p in range(start_page, max_pages + 1):
            page_url = _with_page_param(seed, p)
            if page_url not in seen:
                seen.add(page_url)
                urls.append(page_url)
                if verbose:
                    print(f"[ADD] {page_url}")
            else:
                if verbose:
                    print(f"[SKIP] Duplicada: {page_url}")

    return urls