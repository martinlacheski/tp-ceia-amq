import pandas as pd
import json
from pathlib import Path

def procesar_tiempos_resolucion():
    base_path = Path(__file__).parent.parent
    xls_path = base_path / "data/raw/archivos/Planilla reclamos.xls"
    output_path = base_path / "data/processed/tiempos_resolucion_localidad.json"
    
    print(f"Leyendo planilla desde {xls_path}...")
    df = pd.read_excel(xls_path)
    
    # Construir timestamps
    # Algunos campos pueden venir como datetime o como string, pd.to_datetime maneja ambos
    df["dt_inicio"] = pd.to_datetime(df["Fecha"].astype(str) + " " + df["Hora In"].astype(str), errors="coerce")
    df["dt_fin"] = pd.to_datetime(df["Fecha Fin"].astype(str) + " " + df["Hora Fin"].astype(str), errors="coerce")
    
    # Calcular diferencia en minutos
    df["diff_min"] = (df["dt_fin"] - df["dt_inicio"]).dt.total_seconds() / 60
    
    # Limpieza de datos:
    # 1. Eliminar negativos (errores de carga)
    # 2. Eliminar outliers extremos (ej. más de 24 horas para un solo reclamo operativo in-situ)
    valid = df[(df["diff_min"] > 0) & (df["diff_min"] <= 1440)].copy()
    
    # Normalizar nombres de localidad para que coincidan con el dataset principal (UPPERCASE + nombres canónicos)
    NORMALIZACION = {
        "Montecarlo": "MONTECARLO",
        "Pto. Piray": "PUERTO PIRAY",
        "El Alcázar": "EL ALCAZAR",
        "Caraguatay": "CARAGUATAY"
    }
    valid["Localidad_Norm"] = valid["Localidad"].map(NORMALIZACION)
    
    # Agrupar por localidad normalizada y sacar la mediana
    stats = valid.groupby("Localidad_Norm")["diff_min"].median().to_dict()
    
    # Calcular mediana global para usar como fallback
    global_median = valid["diff_min"].median()
    stats["GLOBAL_FALLBACK"] = global_median
    
    print("Estadísticas calculadas (medianas en minutos):")
    for loc, val in stats.items():
        print(f" - {loc}: {val:.2f} min")
        
    # Guardar a JSON
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
        
    print(f"Mapeo guardado en {output_path}")

if __name__ == "__main__":
    procesar_tiempos_resolucion()
