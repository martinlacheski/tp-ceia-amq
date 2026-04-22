from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import pandas as pd

from helpers.paths import INTERMEDIATE_DIR, PROCESSED_DIR, REPORTS_DIR


DATASET_PATHS: dict[str, Path] = {
    "zona_diario_supervisado": PROCESSED_DIR / "zona_diario_supervisado.parquet",
    "zona_diario_base": PROCESSED_DIR / "zona_diario_base.parquet",
    "reclamos_zonificados": PROCESSED_DIR / "reclamos_zonificados.parquet",
    "lluvia_diaria_clean": PROCESSED_DIR / "lluvia_diaria_clean.parquet",
    "reclamos_clean": PROCESSED_DIR / "reclamos_clean.parquet",
    "exclusiones_reclamos": INTERMEDIATE_DIR / "exclusiones_reclamos.parquet",
    "exclusiones_lluvia": INTERMEDIATE_DIR / "exclusiones_lluvia.parquet",
    "tramites_temporal_audit": INTERMEDIATE_DIR / "tramites_temporal_audit.parquet",
}

REPORT_PATHS: dict[str, Path] = {
    "phase2_cleaning_audit": REPORTS_DIR / "phase2_cleaning_audit.md",
    "phase3_rainfall_audit": REPORTS_DIR / "phase3_rainfall_audit.md",
    "zona_diario_build_summary": REPORTS_DIR / "zona_diario_build_summary.md",
    "rain_feature_notes": REPORTS_DIR / "rain_feature_notes.md",
}

DATE_CANDIDATES = ("fecha", "fecha_reclamo", "fecha_tarea")
SERVICE_CANDIDATES = ("zona_servicio_principal", "servicio_normalizado", "servicio")
SERVICE_DETAIL_CANDIDATES = ("servicio_normalizado", "servicio", "zona_servicio_principal")
ALLOWED_SERVICES = (
    "AGUA POTABLE",
    "ALUMBRADO PUBLICO",
    "CLOACA",
    "ENERGIA",
    "ENERGIA PREPAGA",
    "TRANSMISION DE DATOS",
    "TV AIRE",
    "TV CABLE",
)
ALLOWED_SERVICES_SET = frozenset(ALLOWED_SERVICES)


def build_dataset_inventory(names: list[str] | None = None) -> pd.DataFrame:
    selected = names or list(DATASET_PATHS)
    rows: list[dict[str, object]] = []
    for name in selected:
        path = DATASET_PATHS[name]
        rows.append(
            {
                "dataset": name,
                "path": path.relative_to(PROCESSED_DIR.parents[0]).as_posix(),
                "exists": path.exists(),
                "size_mb": round(path.stat().st_size / (1024 * 1024), 2) if path.exists() else None,
            }
        )
    return pd.DataFrame(rows).sort_values("dataset", kind="stable").reset_index(drop=True)


def load_dataset(name: str, columns: list[str] | None = None) -> pd.DataFrame:
    if name not in DATASET_PATHS:
        raise KeyError(f"Dataset desconocido: {name}")
    path = DATASET_PATHS[name]
    if not path.exists():
        raise FileNotFoundError(f"No existe el dataset requerido: {path}")
    return pd.read_parquet(path, columns=columns)


def read_report(name: str) -> str:
    if name not in REPORT_PATHS:
        raise KeyError(f"Reporte desconocido: {name}")
    path = REPORT_PATHS[name]
    return path.read_text(encoding="utf-8") if path.exists() else ""


def detect_date_column(df: pd.DataFrame, candidates: tuple[str, ...] = DATE_CANDIDATES) -> str | None:
    for column in candidates:
        if column in df.columns:
            return column
    return None


def detect_service_column(df: pd.DataFrame, candidates: tuple[str, ...] = SERVICE_CANDIDATES) -> str | None:
    for column in candidates:
        if column in df.columns:
            return column
    return None


def get_allowed_services() -> list[str]:
    return list(ALLOWED_SERVICES)


def normalize_service_label(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    normalized = str(value).strip().upper()
    return normalized or None


def filter_allowed_services(
    df: pd.DataFrame,
    service_columns: Iterable[str] | None = None,
    *,
    allowed_services: Iterable[str] = ALLOWED_SERVICES,
) -> pd.DataFrame:
    columns = [column for column in (service_columns or SERVICE_DETAIL_CANDIDATES) if column in df.columns]
    if not columns:
        return df.copy()

    allowed = {service for service in (normalize_service_label(value) for value in allowed_services) if service}
    mask = pd.Series(False, index=df.index)
    for column in columns:
        normalized = df[column].map(normalize_service_label)
        mask = mask | normalized.isin(allowed)
    return df.loc[mask].copy()


def resolve_service_dimension(
    df: pd.DataFrame,
    dataset_name: str,
    related_claims_df: pd.DataFrame | None = None,
    min_distinct: int = 3,
) -> dict[str, object]:
    if dataset_name in {"zona_diario_supervisado", "zona_diario_base"} and {"fecha", "zona_id"}.issubset(df.columns):
        claims_df = related_claims_df
        if claims_df is None:
            claims_df = load_dataset(
                "reclamos_zonificados",
                columns=["fecha", "zona_id", "servicio_normalizado", "servicio", "reclamo_id"],
            )
        service_frame = _build_claim_service_frame(claims_df)
        return {
            "mode": "claims_proxy",
            "service_column": "service_label",
            "service_label": "servicio_real",
            "description": "Se usa `reclamos_zonificados` para exponer servicios reales por zona-fecha.",
            "service_frame": service_frame,
        }

    service_column = detect_service_column(df)
    if service_column is not None:
        distinct_services = int(df[service_column].dropna().astype("string").str.strip().replace("", pd.NA).dropna().nunique())
        if distinct_services >= min_distinct:
            service_frame = _build_direct_service_frame(df, service_column)
            return {
                "mode": "direct",
                "service_column": service_column,
                "service_label": service_column,
                "description": f"Filtro directo usando `{service_column}`.",
                "service_frame": service_frame,
            }

    return {
        "mode": "none",
        "service_column": None,
        "service_label": None,
        "description": "No se encontraron columnas de servicio suficientes para filtrar.",
        "service_frame": pd.DataFrame(columns=["fecha", "zona_id", "service_label", "claim_count"]),
    }


def filter_dataframe_by_services(
    df: pd.DataFrame,
    service_dimension: dict[str, object],
    selected_services: Iterable[str],
) -> pd.DataFrame:
    selected = [value for value in (normalize_service_label(service) for service in selected_services) if value]
    if not selected:
        return df

    mode = service_dimension.get("mode")
    if mode == "direct":
        service_column = service_dimension.get("service_column")
        if isinstance(service_column, str) and service_column in df.columns:
            normalized = df[service_column].map(normalize_service_label)
            return df.loc[normalized.isin(selected)].copy()
        return df

    if mode == "claims_proxy" and {"fecha", "zona_id"}.issubset(df.columns):
        service_frame = service_dimension.get("service_frame")
        if not isinstance(service_frame, pd.DataFrame) or service_frame.empty:
            return df.iloc[0:0].copy()
        keys = service_frame.loc[
            service_frame["service_label"].map(normalize_service_label).isin(selected),
            ["fecha", "zona_id"],
        ].drop_duplicates()
        if keys.empty:
            return df.iloc[0:0].copy()
        return df.merge(keys.assign(_service_match=True), on=["fecha", "zona_id"], how="inner").drop(columns="_service_match")

    return df


def normalize_lat_lon(
    df: pd.DataFrame,
    lat_col: str,
    lon_col: str,
    extra_columns: list[str] | None = None,
) -> pd.DataFrame:
    columns = [lat_col, lon_col, *(extra_columns or [])]
    available_columns = [column for column in columns if column in df.columns]
    if lat_col not in df.columns or lon_col not in df.columns:
        return pd.DataFrame(columns=["lat", "lon", *(extra_columns or [])])

    scoped = df[available_columns].copy()
    lat = _coerce_coordinate_series(scoped[lat_col])
    lon = _coerce_coordinate_series(scoped[lon_col])

    swap_mask = lat.abs().gt(90) & lon.abs().le(90)
    lat_fixed = lat.where(~swap_mask, lon)
    lon_fixed = lon.where(~swap_mask, lat)
    valid_mask = lat_fixed.between(-90, 90, inclusive="both") & lon_fixed.between(-180, 180, inclusive="both")

    normalized = pd.DataFrame({"lat": lat_fixed, "lon": lon_fixed}, index=df.index)
    for column in extra_columns or []:
        if column in scoped.columns:
            normalized[column] = scoped[column]
    return normalized.loc[valid_mask].dropna(subset=["lat", "lon"])


def prepare_claim_map_dataset(df: pd.DataFrame, max_points: int = 5000) -> pd.DataFrame:
    extra_columns = [
        column
        for column in ["reclamo_id", "fecha", "zona_id", "sede_id", "servicio_normalizado", "servicio", "motivo"]
        if column in df.columns
    ]
    map_df = normalize_lat_lon(df, lat_col="lat", lon_col="lon", extra_columns=extra_columns)
    if map_df.empty:
        return map_df

    if "fecha" in map_df.columns:
        map_df["fecha"] = pd.to_datetime(map_df["fecha"], errors="coerce")
        map_df = map_df.sort_values("fecha", ascending=False, kind="stable")

    if len(map_df) > max_points:
        map_df = map_df.sample(n=max_points, random_state=42)

    return map_df.reset_index(drop=True)


def prepare_zone_map_dataset(df: pd.DataFrame) -> pd.DataFrame:
    if not {"zona_id", "centroid_lat", "centroid_lon"}.issubset(df.columns):
        return pd.DataFrame(columns=["zona_id", "lat", "lon", "dias", "reclamos_totales"])

    extra_columns = [
        column
        for column in ["zona_id", "reclamos_count", "costo_total_compuesto_ars", "zona_servicio_principal"]
        if column in df.columns
    ]
    zone_points = normalize_lat_lon(df, lat_col="centroid_lat", lon_col="centroid_lon", extra_columns=extra_columns)
    if zone_points.empty:
        return pd.DataFrame(columns=["zona_id", "lat", "lon", "dias", "reclamos_totales"])

    aggregations: dict[str, str] = {"lat": "mean", "lon": "mean"}
    if "reclamos_count" in zone_points.columns:
        aggregations["reclamos_count"] = "sum"
    if "costo_total_compuesto_ars" in zone_points.columns:
        aggregations["costo_total_compuesto_ars"] = "sum"
    if "zona_servicio_principal" in zone_points.columns:
        aggregations["zona_servicio_principal"] = "first"

    grouped = zone_points.groupby("zona_id", as_index=False).agg(aggregations)
    grouped["dias"] = zone_points.groupby("zona_id").size().reindex(grouped["zona_id"]).to_numpy()
    return grouped.rename(
        columns={
            "reclamos_count": "reclamos_totales",
            "costo_total_compuesto_ars": "costo_total_compuesto_ars",
            "zona_servicio_principal": "servicio_principal",
        }
    )


def _build_direct_service_frame(df: pd.DataFrame, service_column: str) -> pd.DataFrame:
    columns = [column for column in [detect_date_column(df), "zona_id", service_column] if column]
    frame = df[columns].copy() if columns else pd.DataFrame(index=df.index)
    if "fecha" not in frame.columns and detect_date_column(df) is not None:
        frame["fecha"] = pd.to_datetime(df[detect_date_column(df)], errors="coerce")
    if "zona_id" not in frame.columns:
        frame["zona_id"] = pd.NA
    frame["service_label"] = df[service_column].map(normalize_service_label)
    frame["claim_count"] = 1
    return frame.loc[frame["service_label"].isin(ALLOWED_SERVICES_SET)].reset_index(drop=True)


def _build_claim_service_frame(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy()
    if "fecha" in working.columns:
        working["fecha"] = pd.to_datetime(working["fecha"], errors="coerce")
    working["service_label"] = _coalesce_service_columns(working)
    working = working.loc[
        working["service_label"].isin(ALLOWED_SERVICES_SET)
        & working["fecha"].notna()
        & working["zona_id"].notna()
    ].copy()
    if working.empty:
        return pd.DataFrame(columns=["fecha", "zona_id", "service_label", "claim_count"])

    count_column = "reclamo_id" if "reclamo_id" in working.columns else "service_label"
    return (
        working.groupby(["fecha", "zona_id", "service_label"], as_index=False)
        .agg(claim_count=(count_column, "nunique"))
        .sort_values(["claim_count", "service_label"], ascending=[False, True], kind="stable")
        .reset_index(drop=True)
    )


def _coalesce_service_columns(df: pd.DataFrame) -> pd.Series:
    service_series = pd.Series(pd.NA, index=df.index, dtype="string")
    for column in SERVICE_DETAIL_CANDIDATES:
        if column not in df.columns:
            continue
        candidate = df[column].map(normalize_service_label).astype("string")
        service_series = service_series.fillna(candidate)
    return service_series


def _coerce_coordinate_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype("string").str.replace(",", ".", regex=False).str.strip(), errors="coerce")


def dataset_overview(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    date_column = detect_date_column(df)
    date_min = pd.NaT
    date_max = pd.NaT
    if date_column is not None:
        dates = pd.to_datetime(df[date_column], errors="coerce")
        date_min = dates.min()
        date_max = dates.max()

    return pd.DataFrame(
        [
            {
                "dataset": dataset_name,
                "rows": len(df),
                "columns": len(df.columns),
                "date_column": date_column or "—",
                "date_min": date_min,
                "date_max": date_max,
                "duplicated_rows": int(df.duplicated().sum()),
            }
        ]
    )


def quality_profile(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    total_rows = max(len(df), 1)
    for column in df.columns:
        series = df[column]
        top_values = series.astype("string").fillna("<NA>").value_counts(dropna=False).head(top_n)
        rows.append(
            {
                "column": column,
                "dtype": str(series.dtype),
                "nulls": int(series.isna().sum()),
                "null_ratio": round(float(series.isna().sum()) / total_rows, 4),
                "distinct": int(series.nunique(dropna=True)),
                "sample_top_values": ", ".join(f"{idx} ({int(value)})" for idx, value in top_values.items()),
            }
        )
    return pd.DataFrame(rows).sort_values(["null_ratio", "distinct", "column"], ascending=[False, False, True], kind="stable")


def numeric_profile(df: pd.DataFrame, columns: list[str] | None = None) -> pd.DataFrame:
    numeric_df = df.select_dtypes(include=["number", "bool"]).copy()
    if columns is not None:
        numeric_df = numeric_df[[column for column in columns if column in numeric_df.columns]]
    if numeric_df.empty:
        return pd.DataFrame()
    described = numeric_df.describe(percentiles=[0.5, 0.9, 0.95, 0.99]).T.reset_index().rename(columns={"index": "column"})
    return described.sort_values("column", kind="stable").reset_index(drop=True)


def categorical_profile(df: pd.DataFrame, columns: list[str], top_n: int = 10) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for column in columns:
        if column not in df.columns:
            continue
        top_values = df[column].astype("string").fillna("<NA>").value_counts(dropna=False).head(top_n)
        for label, count in top_values.items():
            rows.append({"column": column, "value": label, "count": int(count)})
    return pd.DataFrame(rows)


def classify_feature(column: str) -> str:
    if column.startswith("y_"):
        return "target"
    if "_lag_" in column:
        return "lag"
    if "_roll_" in column:
        return "rolling"
    if column.startswith("lluvia") or column in {"llovio", "observacion_codigo", "obs_categoria", "obs_evento_intenso_flag"}:
        return "weather"
    if column in {"fecha", "day_of_week", "month", "quarter", "is_weekend", "zona_id", "cluster_root"}:
        return "calendar_or_key"
    if column.startswith("zona_") or column.startswith("reclamos_"):
        return "zone_or_claims"
    return "base_feature"


def supervised_feature_inventory(base_df: pd.DataFrame, supervised_df: pd.DataFrame) -> pd.DataFrame:
    base_columns = set(base_df.columns)
    rows: list[dict[str, object]] = []
    total_rows = max(len(supervised_df), 1)
    for column in supervised_df.columns:
        rows.append(
            {
                "column": column,
                "feature_family": classify_feature(column),
                "introduced_in_supervised": column not in base_columns,
                "nulls": int(supervised_df[column].isna().sum()),
                "coverage": round(1 - float(supervised_df[column].isna().sum()) / total_rows, 4),
            }
        )
    return pd.DataFrame(rows).sort_values(["introduced_in_supervised", "feature_family", "column"], ascending=[False, True, True], kind="stable")


def preparation_trace(base_df: pd.DataFrame, supervised_df: pd.DataFrame) -> pd.DataFrame:
    added_columns = [column for column in supervised_df.columns if column not in base_df.columns]
    rows = [
        {
            "step": "base_dataset",
            "rows": int(len(base_df)),
            "columns": int(len(base_df.columns)),
            "detail": "Calendario zona-fecha consolidado con reclamos, costos, tiempos y lluvia.",
        },
        {
            "step": "feature_engineering_supervisado",
            "rows": int(len(supervised_df)),
            "columns": int(len(supervised_df.columns)),
            "detail": f"Se agregan {len(added_columns)} columnas nuevas: lags, rolling means y targets t+1.",
        },
        {
            "step": "rows_with_target_t+1",
            "rows": int(supervised_df.get("target_available_t+1", pd.Series(False, index=supervised_df.index)).sum()),
            "columns": 3,
            "detail": "Filas listas para baseline/modelado posterior sin leakage hacia el futuro.",
        },
    ]
    return pd.DataFrame(rows)
