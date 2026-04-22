from __future__ import annotations

from collections.abc import Iterable

import pandas as pd


TRAMITES_REQUIRED_COLUMNS = [
    "NumeroTramite",
    "NumeroOrden",
    "soccod",
    "posloc",
    "dirdes",
    "geo1",
    "geo2",
    "serdes",
    "descrmotivo",
    "fechainicio",
    "horainicio",
    "fechafin",
    "horafin",
    "dt_inicio",
    "dt_fin",
]

TRAMITES_TAREAS_REQUIRED_COLUMNS = [
    "NumeroTramite",
    "NumeroOrden",
    "fecha",
    "hora",
    "codigotar",
    "tarea",
]

PROCESSED_CONTRACTS: dict[str, list[str]] = {
    "zonas_clusterizadas": ["destino_key", "lat", "lon", "zona_id"],
    "zona_cluster_resumen": ["zona_id", "destinos_count", "reclamos_count", "diameter_km"],
    "reclamos_zonificados": ["reclamo_id", "fecha_reclamo", "zona_id", "sede_id", "distance_km", "costo_operativo_ars"],
    "reclamos_clean": ["reclamo_id", "fecha_reclamo", "lat", "lon", "motivo", "estado_geo"],
    "tareas_clean": ["reclamo_id", "tarea_id", "fecha_tarea", "tipo_tarea"],
    "destinos_unicos": ["destino_key", "lat", "lon", "reclamos_count"],
    "costos_ref": ["vehiculo_tipo", "costo_km", "costo_hora"],
    "sede_ref": ["sede_id", "sede_nombre", "lat", "lon"],
    "servicio_sede_ref": ["servicio", "servicio_normalizado", "sede_id"],
    "distancias": ["reclamo_id", "distance_m", "duration_s", "routing_status", "fuente_ruteo"],
    "distancias_costos": [
        "reclamo_id",
        "distance_m",
        "duration_s",
        "distance_km",
        "duration_h",
        "costo_km_ars",
        "costo_hora_ars",
        "costo_operativo_ars",
    ],
    "reclamos_enriquecidos": [
        "reclamo_id",
        "fecha_reclamo",
        "lat",
        "lon",
        "destino_key",
        "routing_status",
        "distance_m",
        "duration_s",
        "costo_operativo_ars",
    ],
    "lluvia_diaria": ["fecha", "lluvia_mm", "lluvia_status"],
    "resumen_operativo_diario": [
        "fecha",
        "reclamos_total",
        "reclamos_ruteados_ok",
        "destinos_unicos",
        "costo_operativo_total_ars",
        "lluvia_mm",
        "lluvia_status",
    ],
    "resumen_hotspots": [
        "destino_key",
        "lat",
        "lon",
        "reclamos_total",
        "reclamos_ruteados_ok",
        "costo_operativo_total_ars",
    ],
    "zona_diario_base": [
        "zona_id",
        "fecha",
        "reclamos_count",
        "traslado_min_total",
        "resolucion_base_min_total",
        "tiempo_total_operativo_min",
        "costo_total_compuesto_ars",
        "lluvia_mm",
        "llovio",
    ],
    "zona_diario_supervisado": [
        "zona_id",
        "fecha",
        "reclamos_count",
        "y_tiempo_t+1_min",
        "y_costo_t+1_ars",
        "y_reclamos_t+1",
    ],
    "zona_diario_baseline_t1": [
        "zona_id",
        "fecha",
        "baseline_tiempo_t+1_min",
        "baseline_costo_t+1_ars",
        "baseline_reclamos_t+1",
        "y_tiempo_t+1_min",
        "y_costo_t+1_ars",
        "y_reclamos_t+1",
    ],
    "zona_diario_baseline_metricas": [
        "target",
        "evaluation_window",
        "rows",
        "mae",
        "rmse",
        "wmape",
    ],
}


def validate_required_columns(df: pd.DataFrame, required_columns: Iterable[str], dataset_name: str) -> None:
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise ValueError(f"{dataset_name} no cumple contrato mínimo. Faltan columnas: {missing}")


def validate_raw_inputs(tramites_df: pd.DataFrame, tareas_df: pd.DataFrame) -> None:
    validate_required_columns(tramites_df, TRAMITES_REQUIRED_COLUMNS, "tramites")
    validate_required_columns(tareas_df, TRAMITES_TAREAS_REQUIRED_COLUMNS, "tramites_tareas")


def validate_processed_contract(df: pd.DataFrame, contract_name: str) -> None:
    if contract_name not in PROCESSED_CONTRACTS:
        raise KeyError(f"Contrato procesado desconocido: {contract_name}")
    validate_required_columns(df, PROCESSED_CONTRACTS[contract_name], contract_name)


def schema_profile(df: pd.DataFrame) -> pd.DataFrame:
    profile = pd.DataFrame(
        {
            "column": df.columns,
            "dtype": [str(dtype) for dtype in df.dtypes],
            "nulls": [int(df[column].isna().sum()) for column in df.columns],
        }
    )
    profile["null_ratio"] = (profile["nulls"] / max(len(df), 1)).round(4)
    return profile
