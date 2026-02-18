import json
from pathlib import Path

# Ruta base del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent

# Archivo donde se guardará el provider actual (en /data)
CURRENT_PROVIDER_FILE = BASE_DIR / "data" / "current_provider.json"

CATALOG_ID = 31  # valor fijo para todos los SPs
PROVIDER_ID_LIST = [292] # providerID de los vendor a auditar

from pydantic_settings import BaseSettings
from typing import List
 
class Settings(BaseSettings):
    DB_SERVER: str
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    DB_SP_NAME: str

    class Config:
        env_file = ".env"
 
settings = Settings()