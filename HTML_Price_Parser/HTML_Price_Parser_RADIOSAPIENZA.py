import os
import json
import pandas as pd
from datetime import datetime

DATA_STORAGE_DIR = "DataStorage"
INPUT_PREFIX = "radiosapienza_html_"
OUTPUT_PREFIX = "radiosapienza_prices"

def get_latest_html_csv():
    files = [
        f for f in os.listdir(DATA_STORAGE_DIR)
        if f.startswith(INPUT_PREFIX) and f.endswith(".csv")
    ]
    if not files:
        raise FileNotFoundError(f"No se encontraron archivos '{INPUT_PREFIX}*.csv' en {DATA_STORAGE_DIR}")

    return max((os.path.join(DATA_STORAGE_DIR, f) for f in files), key=os.path.getmtime)

def coerce_to_json_obj(payload: str):
    """
    Convierte el contenido de la columna 'html' a dict JSON, soportando:
    - JSON normal: {"status":"OK",...}
    - JSON con comillas duplicadas: {""status"":""OK"",...}
    - JSON doblemente escapado: "{""status"":""OK"",...}"  (string que contiene JSON)
    """
    if payload is None:
        return None

    s = str(payload).strip()
    if not s or s.lower() == "nan":
        return None

    # 1) Intento directo
    try:
        obj = json.loads(s)
        # Si esto devuelve un string (JSON doblemente escapado), lo parseamos de nuevo
        if isinstance(obj, str):
            try:
                return json.loads(obj)
            except json.JSONDecodeError:
                s = obj  # seguimos intentando con transforms
        else:
            return obj
    except json.JSONDecodeError:
        pass

    # 2) Arreglar comillas duplicadas típicas del CSV: {""status"":""OK""} -> {"status":"OK"}
    s2 = s.replace('""', '"')
    try:
        obj = json.loads(s2)
        if isinstance(obj, str):
            return json.loads(obj)
        return obj
    except json.JSONDecodeError:
        pass

    # 3) Último intento: si está envuelto en comillas externas, lo “des-escapamos”
    # Ej: "\"{\\\"status\\\":\\\"OK\\\"}\"" -> "{...}"
    try:
        unescaped = json.loads(s)  # si s es un string JSON, esto lo convierte a texto plano
        if isinstance(unescaped, str):
            unescaped = unescaped.replace('""', '"')
            return json.loads(unescaped)
    except Exception:
        return None

    return None

def extract_precio_from_json(payload: str, sku_vendor: str):
    data = coerce_to_json_obj(payload)
    if not isinstance(data, dict):
        return None

    try:
        productos = data.get("result", {}).get("productos", [])
        if not productos:
            return None

        # Preferimos el producto cuyo codigo_productos == sku_vendor
        chosen = None
        for p in productos:
            if str(p.get("codigo_productos", "")).strip() == str(sku_vendor).strip():
                chosen = p
                break
        if chosen is None:
            chosen = productos[0]

        precio = chosen.get("precio")
        if precio is None or str(precio).strip() == "":
            return None

        # En tu ejemplo viene "89999"
        return int(float(str(precio).strip()))
    except Exception:
        return None

def main():
    input_csv = get_latest_html_csv()
    print(f"Usando archivo: {input_csv}")

    # Mantener strings tal cual, sin NaN automáticos
    df = pd.read_csv(input_csv, dtype=str, keep_default_na=False)

    out = []
    for _, row in df.iterrows():
        sku_db = row.get("SKUdb", "")
        sku_vendor = row.get("SKUvendor", "")
        payload = row.get("html", "")

        precio = extract_precio_from_json(payload, sku_vendor)

        out.append({
            "SKUdb": sku_db,
            "SKUvendor": sku_vendor,
            "precio": precio
        })

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_csv = f"{DATA_STORAGE_DIR}/{OUTPUT_PREFIX}_{ts}.csv"
    pd.DataFrame(out).to_csv(output_csv, index=False)

    print(f"✔ CSV generado: {output_csv}")

if __name__ == "__main__":
    main()

