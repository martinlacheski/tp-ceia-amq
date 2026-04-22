import os
from google import genai
from pydantic import BaseModel, Field
from dotenv import load_dotenv

# Load env vars
load_dotenv()

# Initialize Gemini Client
client = genai.Client()

class DailyRecord(BaseModel):
    dia: int = Field(description="El número de día del 1 al 31.")
    cantidad_mm: float = Field(description="La cantidad de lluvia en mm. 0.0 si está vacío, es una raya, o tiene cualquier letra u otro símbolo indicando 'sin lluvia'.")

class MonthlyRecord(BaseModel):
    año: int = Field(description="El año indicado en la cabecera de la planilla.")
    mes: str = Field(description="El mes indicado en la cabecera de la planilla.")
    registros: list[DailyRecord] = Field(description="Los valores diarios para los días que figuran en la lista.")

image_path = "TP/Registro Pluviometrico Montecarlo/Lluvias 2024/1. registro lluvia enero 2024 001.jpg"

try:
    image_file = client.files.upload(file=image_path)
    
    prompt = """
    Extraé los datos de esta planilla de 'REGISTRO PLUVIOMETRICO'.
    Identificá el mes y el año en la cabecera.
    Luego, leé las columnas 'DIAS' y 'Cantidad en mm' de la tabla 'PRECIPITACION' (la de la derecha, a veces dice 'Cantidad en mm').
    Fijate con atención, en la imagen a veces hay una 'A' cuando no llovió, un guión o vacío. En esos casos, el valor es 0.0.
    Si hay una anotación como '7,1', el valor es 7.1. Usá punto decimal si aplica.
    Devolvé SOLAMENTE los registros, en formato JSON usando la estructura pedida.
    """
    
    # Intento con gemini-2.5-flash, si falla uso 2.0-flash
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
    
    print(response.text)

except Exception as e:
    print("Error:", e)
