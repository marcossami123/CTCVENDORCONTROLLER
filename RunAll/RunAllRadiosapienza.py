import subprocess
import sys
from pathlib import Path

# =========================================================
# Raíz del proyecto (ctcVendorController)
# =========================================================
BASE_DIR = Path(__file__).resolve().parent.parent

# =========================================================
# Scripts a ejecutar (ORDEN DEFINIDO POR VOS)
# =========================================================
scripts = [
    BASE_DIR / "db_connector" / "fetchdataRADIOSAPIENZA.py",
    BASE_DIR / "Megatone" / "GetInfoAPIRadiosapienza.py",
    BASE_DIR / "HTML_Price_Parser" / "HTML_Price_Parser_RADIOSAPIENZA.py",
    BASE_DIR / "Audit" / "PriceComparisonRadioSapienza.py",
    BASE_DIR / "ShowHTML" / "ShowHTML_ICBC.py",
    BASE_DIR / "HTML_Price_Parser" / "HTML_Price_Parser_ICBC.py",
    BASE_DIR / "Audit" / "PriceComparisonICBC.py",
    BASE_DIR / "BNA" / "api_caller_BNA.py",
    BASE_DIR / "Audit" / "PriceComparisonBNA.py"
]

def run_script(script_path: Path):
    if not script_path.exists():
        raise FileNotFoundError(f"❌ No existe el script: {script_path}")

    print(f"\n▶ Ejecutando {script_path.name}")

    subprocess.run(
        [sys.executable, str(script_path)],
        cwd=BASE_DIR,   # 🔥 siempre desde la raíz del proyecto
        check=True
    )

def main():
    for script in scripts:
        run_script(script)

    print("\n✅ Proceso completo Radio Sapienza finalizado OK")

if __name__ == "__main__":
    main()