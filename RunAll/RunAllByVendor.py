import os
import sys
import subprocess
from pathlib import Path

# ---------------------------------------------------------
# Resolver raíz del proyecto
# ---------------------------------------------------------
BASE_DIR = Path(__file__).resolve()
while BASE_DIR.name != "CTCVENDORCONTROLLER":
    BASE_DIR = BASE_DIR.parent
    if BASE_DIR.parent == BASE_DIR:
        raise RuntimeError("No se encontró la raíz del proyecto CTCVENDORCONTROLLER")

RUNALL_DIR = BASE_DIR / "RunAll"

# ---------------------------------------------------------
# Registro de vendors soportados
# ---------------------------------------------------------
VENDORS = {
    "megatone": {
        "script": "RunAllMegatone.py",
    },
    "radiosapienza": {
        "script": "RunAllRadiosapienza.py",
    },
    "vstore": {
        "script": "RunAllVstore.py",
    },
    "emood": {
        "script": "RunAllEmood.py",
    },
}

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def main():
    vendor = os.getenv("TARGET_VENDOR")

    if not vendor:
        raise RuntimeError("No se definió la variable de entorno TARGET_VENDOR")

    vendor = vendor.lower().strip()

    if vendor not in VENDORS:
        raise ValueError(
            f"Vendor no soportado: {vendor}\n"
            f"Vendors disponibles: {', '.join(VENDORS.keys())}"
        )

    script_name = VENDORS[vendor]["script"]
    script_path = RUNALL_DIR / script_name

    if not script_path.exists():
        raise FileNotFoundError(f"No se encontró el script: {script_path}")

    print(f"Ejecutando pipeline para vendor: {vendor}")
    print(f"Script: {script_name}\n")

    subprocess.run(
        [sys.executable, str(script_path)],
        check=True
    )

# ---------------------------------------------------------
# Entry point
# ---------------------------------------------------------
if __name__ == "__main__":
    main()

