# config/bna_api_config.py

"""
Configuración de la API de Tienda BNA.

Este archivo contiene exclusivamente:
- URL
- headers
- parámetros base
"""

BNA_API = {
    "url": "https://api-bna.avenida.com/api/angular_app/search/",
    "method": "GET",
    "headers": {
        "Origin": "https://www.tiendabna.com.ar",
        "Api-Key": "J0NdKyZt9Kf96Eny96RMHQ",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.tiendabna.com.ar/",
        "Host": "api-bna.avenida.com",
}}