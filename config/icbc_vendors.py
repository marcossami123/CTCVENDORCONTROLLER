"""
Configuración de vendors que venden en ICBC Mall.

Este archivo es la fuente única de verdad para:
- fetch de productos desde BD
- armado de URL ICBC
- normalización de SKUs
- audit de precios

El código NO debe hardcodear vendors ni IDs.
"""

ICBC_VENDORS = {
    "vstore": {
        "vendor_name": "Vstore",
        "vendor_id": 405,
        "catalog_id": 31,
        "sku_strip_chars": 5,
        "vendorid_BNA": 1969
    },
    "emood": {
        "vendor_name": "Emood",
        "vendor_id": 436,
        "catalog_id": 31,
        "productos_filename_prefix": "ProductosEmood",
        "sku_strip_chars": 5,
        "vendorid_BNA": 100000
    },
    "megatone": {
        "vendor_name": "Megatone",
        "vendor_id": 292,
        "catalog_id": 31,
        "sku_strip_chars": 5,
        "vendorid_BNA": 2180
    },
    "diggit": {
        "vendor_name": "Diggit",
        "vendor_id": 342,
        "catalog_id": 31,
        "sku_strip_chars": 5,
        "vendorid_BNA": 3078
    },
    "radio sapienza": {
        "vendor_name": "Radio Sapienza",
        "vendor_id": 450,
        "catalog_id": 31,
        "sku_strip_chars": 5,
        "vendorid_BNA": 1670
    },
    "ceven": {
        "vendor_name": "Ceven",
        "vendor_id": 431,
        "catalog_id": 31,
        "sku_strip_chars": 5,
        "vendorid_BNA": 2083
    },
    "24 store": {
        "vendor_name": "24 Store",
        "vendor_id": 427,
        "catalog_id": 31,
        "sku_strip_chars": 5,
        "vendorid_BNA": 1589
    }
}
