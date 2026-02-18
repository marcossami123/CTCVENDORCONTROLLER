import subprocess
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np

# =========================================================
# BASE DIR
# =========================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_STORAGE_DIR = BASE_DIR / "DataStorage"

print("\n==============================")
print("🚀 RUN ALL – MEGATONE")
print("==============================")

# =========================================================
# SCRIPTS A EJECUTAR (ORDEN CRÍTICO)
# =========================================================

scripts = [
    BASE_DIR / "db_connector" / "fetchdataMEGATONE.py",

    BASE_DIR / "Megatone" / "GetInfoAPIMegatone.py",
    BASE_DIR / "Audit" / "PriceComparisonMegatone.py",

    BASE_DIR / "ShowHTML" / "ShowHTML_ICBC.py",
    BASE_DIR / "HTML_Price_Parser" / "HTML_Price_Parser_ICBC.py",
    BASE_DIR / "Audit" / "PriceComparisonICBC.py",

    BASE_DIR / "BNA" / "api_caller_BNA.py",
    BASE_DIR / "Audit" / "PriceComparisonBNA.py",
]

# =========================================================
# HELPERS
# =========================================================

def run_script(script_path: Path):
    print(f"\n▶️ Ejecutando: {script_path.relative_to(BASE_DIR)}")
    subprocess.run(
        [sys.executable, str(script_path)],
        check=True
    )


def get_latest_file(pattern: str) -> Path:
    files = list(DATA_STORAGE_DIR.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No se encontraron archivos {pattern}")
    return max(files, key=lambda f: f.stat().st_mtime)


# =========================================================
# AUDIT GLOBAL (SIMPLIFICADO + RENOMBRES)
# =========================================================

def run_global_audit():
    print("\n📊 Generando AUDIT GLOBAL (FINAL)")

    # -------------------------
    # Audit Megatone (BASE)
    # -------------------------
    meg_file = get_latest_file("Audit_Megatone_*.xlsx")
    print(f"📄 Megatone : {meg_file.name}")
    df_mega = pd.read_excel(meg_file)
    df_mega.columns = df_mega.columns.str.strip()

    # Renombrar columnas Web del Vendor
    rename_mega = {
        "Diferencia_Absoluta": "Diferencia Absoluta WEB DEL VENDOR",
        "Diferencia_Porcentual": "Diferencia Porcentual WEB DEL VENDOR",
    }
    df_mega = df_mega.rename(columns={k: v for k, v in rename_mega.items() if k in df_mega.columns})

    # -------------------------
    # Audit ICBC (E, F, G)
    # -------------------------
    try:
        icbc_file = get_latest_file("Audit_ICBC_*.xlsx")
        print(f"📄 ICBC     : {icbc_file.name}")
        df_icbc_raw = pd.read_excel(icbc_file)

        df_icbc = df_icbc_raw.iloc[:, 4:7]
        df_icbc.columns = [
            "Precio ICBC",
            "Diferencia Absoluta ICBC",
            "Diferencia Porcentual ICBC"
        ]

    except Exception as e:
        print(f"⚠️ ICBC no disponible ({e}). Se agregan columnas vacías.")
        df_icbc = pd.DataFrame(
            np.nan,
            index=df_mega.index,
            columns=[
                "Precio ICBC",
                "Diferencia Absoluta ICBC",
                "Diferencia Porcentual ICBC"
            ]
        )

    # -------------------------
    # Audit BNA (E, F, G)
    # -------------------------
    try:
        bna_file = get_latest_file("Audit_BNA_*.xlsx")
        print(f"📄 BNA      : {bna_file.name}")
        df_bna_raw = pd.read_excel(bna_file)

        df_bna = df_bna_raw.iloc[:, 4:7]
        df_bna.columns = [
            "Precio BNA",
            "Diferencia Absoluta BNA",
            "Diferencia Porcentual BNA"
        ]

    except Exception as e:
        print(f"⚠️ BNA no disponible ({e}). Se agregan columnas vacías.")
        df_bna = pd.DataFrame(
            np.nan,
            index=df_mega.index,
            columns=[
                "Precio BNA",
                "Diferencia Absoluta BNA",
                "Diferencia Porcentual BNA"
            ]
        )

    # -------------------------
    # Composición final
    # -------------------------
    df_global = pd.concat(
        [df_mega, df_icbc, df_bna],
        axis=1
    )

    # -------------------------
    # OUTPUT
    # -------------------------
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = DATA_STORAGE_DIR / f"Audit_GLOBAL_Megatone_{ts}.csv"
    xlsx_path = DATA_STORAGE_DIR / f"Audit_GLOBAL_Megatone_{ts}.xlsx"

    df_global.to_csv(csv_path, index=False)
    df_global.to_excel(xlsx_path, index=False)

    print("✅ Audit GLOBAL generado (FINAL)")
    print(f"📄 {csv_path.name}")
    print(f"📄 {xlsx_path.name}")


# =========================================================
# MAIN
# =========================================================

def main():
    for script in scripts:
        run_script(script)

    run_global_audit()

    print("\n==============================")
    print("✅ RUN ALL MEGATONE FINALIZADO OK")
    print("📊 Auditorías:")
    print("   - Megatone (base)")
    print("   - ICBC (parseado)")
    print("   - BNA (parseado)")
    print("   - GLOBAL")
    print("==============================\n")


# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    main()



