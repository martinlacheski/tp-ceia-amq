import os
import glob
import pandas as pd
import json
import time
from google import genai
from pydantic import BaseModel, Field
from dotenv import load_dotenv

# Load env vars
load_dotenv()
client = genai.Client()

class DailyRecord(BaseModel):
    dia: int = Field(description="El número de día del 1 al 31.")
    cantidad_mm: float = Field(description="Cantidad de lluvia en mm. 0.0 si la celda está vacía, es guión, o tiene solo letras.")
    observacion: str = Field(description="El texto, letra o código (ej. A, R°, A/B°) de la columna 'OBSERVACIONES'. Si está en blanco, usar string vacío ''")

class MonthlyRecord(BaseModel):
    año: int = Field(description="El año en la cabecera.")
    mes: str = Field(description="El mes en la cabecera.")
    leyenda_referencias: str = Field(description="Si en algún margen o recuadro de esta planilla anotaron el significado de los códigos (qué significa A, R, B...), transcribilo acá. Si no ves explicación alguna, dejá string vacío ''.")
    registros: list[DailyRecord] = Field(description="Registros diarios.")

prompt = """
Sos un sistema OCR experto para datos meteorológicos manuscritos.
Vas a hacerme un doble laburo en esta planilla 'REGISTRO PLUVIOMETRICO':
1. Extraeme todos los registros diarios, día por día. Dame 'cantidad_mm' asumiendo punto decimal (ej: 7.1). Y agarrame la columna de 'OBSERVACIONES' para ese día, para saber qué letras/códigos usaron (A, R°, etc.).
2. Muy importante: Hacé un barrido visual por TODO el documento. Buscá si hay algún rinconcito, pie de página o texto que funcione como Leyenda O Referencia explicando qué significan estos símbolos. Si lo encontrás, copialo en 'leyenda_referencias'. Si no, ponelo vacío. NUNCA adivines un significado, solo copialo si alguien lo dejó explícitamente escrito.
"""

base_dir = "TP/Registro Pluviometrico Montecarlo"

# Agregamos TODAS las extensiones para que no se nos escape Enero 2026 (.jpeg) u otras
image_files = glob.glob(f"{base_dir}/**/*.jpg", recursive=True)
image_files += glob.glob(f"{base_dir}/**/*.jpeg", recursive=True)
image_files += glob.glob(f"{base_dir}/**/*.bmp", recursive=True)
image_files = sorted(image_files)

all_records = []

print(f"\n=======================================================")
print(f"INICIANDO EXTRACCIÓN V2 - OBSERVACIONES Y CAZA-LEYENDAS")
print(f"Archivos encontrados (incluyendo .jpeg): {len(image_files)}")
print(f"=======================================================\n")

for i, img_path in enumerate(image_files):
    # Skipear manuales fijos (no tienen dias)
    if "anual" in img_path.lower():
        continue
        
    print(f"-> Procesando ({i+1}/{len(image_files)}): {os.path.basename(img_path)}")
    try:
        img_uploaded = client.files.upload(file=img_path)
        
        try:
            model_name = 'gemini-2.5-flash'
            response = client.models.generate_content(
                model=model_name,
                contents=[img_uploaded, prompt],
                config={'response_mime_type': 'application/json', 'response_schema': MonthlyRecord, 'temperature': 0.1}
            )
        except Exception:
            model_name = 'gemini-2.0-flash'
            response = client.models.generate_content(
                model=model_name,
                contents=[img_uploaded, prompt],
                config={'response_mime_type': 'application/json', 'response_schema': MonthlyRecord, 'temperature': 0.1}
            )

        data = json.loads(response.text)
        
        # EL MOMENTO DE LA VERDAD
        leyenda = data.get('leyenda_referencias', "").strip()
        if leyenda:
            print(f"\n!!!! 🚨 ALERTA ALERTA 🚨 !!!!")
            print(f"¡Se encontró un texto de referencias en {os.path.basename(img_path)}!")
            print(f"TEXTO EXACTO: '{leyenda}'")
            print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
            
        for p in data.get('registros', []):
            all_records.append({
                'archivo': os.path.basename(img_path),
                'año_extraido': data.get('año'),
                'mes_extraido': data.get('mes'),
                'dia': p.get('dia'),
                'lluvia_mm': p.get('cantidad_mm'),
                'observacion_codigo': p.get('observacion')
            })
            
        print(f"   [OK] Mes: {data.get('mes')} {data.get('año')}")
        
        try:
            client.files.delete(name=img_uploaded.name)
        except Exception:
            pass
            
        time.sleep(2) # Prevent rate limits
        
    except Exception as e:
        print(f"   [ERROR] falló la extracción: {e}")

df = pd.DataFrame(all_records)
output_file = os.path.join(base_dir, "dataset_lluvias_diario_con_obs.csv")
df.to_csv(output_file, index=False)

print(f"\nFIN DEL PROCEDIMIENTO.")
print(f"Dataset guardado en: {output_file}")
print(f"Filas totales extraídas: {len(df)}")
