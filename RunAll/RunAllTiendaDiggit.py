import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

def run_script(script_path: Path):
    print(f"\n▶ Ejecutando {script_path.name}")
    subprocess.run(
        [sys.executable, str(script_path)],
        cwd=BASE_DIR,
        check=True
    )

def main():
    scripts = [
        BASE_DIR / "db_connector" / "fetchdataTIENDADIGGIT.py",
        BASE_DIR / "LinkCreator" / "LinkCreatorDiggit.py",
        BASE_DIR / "ShowHTML" / "ShowHTML_TIENDADIGGIT.py",
        BASE_DIR / "HTML_Price_Parser" / "HTML_Price_Parser_TIENDADIGGIT.py",
        BASE_DIR / "Audit" / "PriceComparisonTiendaDiggit.py",
        BASE_DIR / "ShowHTML" / "ShowHTML_ICBC.py",
        BASE_DIR / "HTML_Price_Parser" / "HTML_Price_Parser_ICBC.py",
        BASE_DIR / "Audit" / "PriceComparisonICBC.py",
        BASE_DIR / "BNA" / "api_caller_BNA.py",
        BASE_DIR / "Audit" / "PriceComparisonBNA.py"
    ]

    for script in scripts:
        if not script.exists():
            raise FileNotFoundError(f"No existe el archivo: {script}")
        run_script(script)

    print("\n✅ Proceso Tienda Diggit finalizado correctamente")

if __name__ == "__main__":
    main()