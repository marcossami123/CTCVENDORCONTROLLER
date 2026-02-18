"""
Script ejecutable para lanzar la validación completa de todos los scrapers.
Ejemplo de ejecución:
    python -m validator.scripts.run_pipeline
"""

from validator.src.pipeline import run_full_pipeline
from common.storage import write_log
from common.config import LOG_ALL, LOG_PIPELINE
from datetime import datetime


def main():
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_msg = f"{ts} | Inicia script run_pipeline (validator)\n"

    try:
        summaries = run_full_pipeline(verbose=True)
        log_msg += f"{ts} | run_full_pipeline ejecutado OK | scrapers procesados={len(summaries)}\n"

        for s in summaries:
            log_msg += (f"  - {s['scraper']:<20} "
                        f"missing={s['missing']:<5} extra={s['extra']:<5} diffs={s['diffs']:<5} "
                        f"status={s['status']}\n")

    except Exception as e:
        log_msg += f"{ts} | ERROR run_pipeline | err={e}\n"
        print(f"[ERROR] {e}")
        raise
    finally:
        if LOG_ALL or LOG_PIPELINE:
            write_log("validator_run_pipeline", log_msg, append=True)
        print("✅ Ejecución finalizada.")


if __name__ == "__main__":
    main()
