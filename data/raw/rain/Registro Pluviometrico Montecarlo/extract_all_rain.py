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
    cantidad_mm: float = Field(description="La cantidad de lluvia en mm. 0.0 si está vacío, es una raya, o tiene una letra (como la A) indicando que no llovió.")

class MonthlyRecord(BaseModel):
    año: int = Field(description="El año en la cabecera.")
    mes: str = Field(description="El mes en la cabecera (texto).")
    registros: list[DailyRecord] = Field(description="Los valores diarios extraídos de la planilla.")

prompt = """
Extraé los datos de esta planilla de 'REGISTRO PLUVIOMETRICO'.
Identificá el mes y el año en la cabecera.
Extraé los valores de la tabla de la derecha llamada 'PRECIPITACION', de la columna 'Cantidad en mm' o similar.
Para cada día del mes anotado (suele ir del 1 al 31), extraé la cantidad de lluvia en milímetros. 
Si la celda está vacía, tiene un guión o una 'A' (o cualquier otra letra suelta), el valor debe ser 0.0.
Si dice algo numérico como '7,1', el valor es 7.1. Usá punto decimal siempre.
Ignorá la fila de 'SUMAS' al final. Devolvé la estructura completa en JSON validado para Pydantic.
"""

base_dir = "TP/Registro Pluviometrico Montecarlo"
image_files = glob.glob(f"{base_dir}/**/*.jpg", recursive=True)
image_files += glob.glob(f"{base_dir}/**/*.bmp", recursive=True)
image_files = sorted(image_files)

all_records = []

print(f"Encontradas {len(image_files)} imágenes en total para procesar.")

for i, img_path in enumerate(image_files):
    print(f"Procesando {i+1}/{len(image_files)}: {img_path}")
    
    # Skipeamos resúmenes anuales que no tienen días
    if "anual" in img_path.lower():
        print("  --> Es un registro anual (resumen). Se omite para datos diarios.")
        continue
        
    try:
        # Subimos el archivo a la API de Gemini
        image_file = client.files.upload(file=img_path)
        
        try:
            model_name = 'gemini-2.5-flash'
            response = client.models.generate_content(
                model=model_name,
                contents=[image_file, prompt],
                config={'response_mime_type': 'application/json', 'response_schema': MonthlyRecord, 'temperature': 0.1}
            )
        except Exception as e:
            model_name = 'gemini-2.0-flash'
            response = client.models.generate_content(
                model=model_name,
                contents=[image_file, prompt],
                config={'response_mime_type': 'application/json', 'response_schema': MonthlyRecord, 'temperature': 0.1}
            )

        data = json.loads(response.text)
        year = data.get('año')
        month = data.get('mes')
        
        registros_procesados = 0
        for p in data.get('registros', []):
            all_records.append({
                'archivo': os.path.basename(img_path),
                'año_extraido': year,
                'mes_extraido': month,
                'dia': p['dia'],
                'lluvia_mm': p['cantidad_mm']
            })
            registros_procesados += 1
            
        print(f"  --> Ok: {month} {year} | {registros_procesados} días extraídos.")
        
        # Eliminar el archivo de la API para no acumular basura (opcional)
        client.files.delete(name=image_file.name)
        
        # Una pequeña pausa para respetar rate limits de la capa gratuita
        time.sleep(2)
        
    except Exception as e:
        print(f"  --> Error al procesar {img_path}: {e}")

df = pd.DataFrame(all_records)
output_file = os.path.join(base_dir, "dataset_lluvias_diario.csv")
df.to_csv(output_file, index=False)
print(f"\n======================================")
print(f"Proceso completado. Datos guardados en:")
print(f"{output_file}")
print(f"Total registros extraidos: {len(df)}")
print(f"======================================")
