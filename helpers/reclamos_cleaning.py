from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from helpers.contracts import (
    validate_processed_contract,
    validate_raw_inputs,
)
from helpers.paths import INTERMEDIATE_DIR, PROCESSED_DIR, REPORTS_DIR, ensure_workspace_layout, require_input


DATE_WINDOW_START = pd.Timestamp("2021-01-01 00:00:00")
DATE_WINDOW_END = pd.Timestamp("2026-03-31 23:59:59")
LAT_BOUNDS = (-30.0, -25.0)
LON_BOUNDS = (-57.0, -53.0)


@dataclass(slots=True)
class Phase2Artifacts:
    reclamos_clean: Path
    tareas_clean: Path
    destinos_unicos: Path
    exclusiones_reclamos: Path
    exclusiones_tareas: Path
    tramites_temporal_audit: Path
    report: Path


def _normalize_hhmm(value: object) -> str | None:
    if pd.isna(value):
        return None
    digits = "".join(character for character in str(value).strip() if character.isdigit())
    if not digits:
        return None
    digits = digits.zfill(4)[-4:]
    hours = int(digits[:2])
    minutes = int(digits[2:])
    if hours > 23 or minutes > 59:
        return None
    return f"{digits[:2]}:{digits[2:]}"


def _combine_date_and_hhmm(date_series: pd.Series, time_series: pd.Series) -> pd.Series:
    date_values = pd.to_datetime(date_series, errors="coerce")
    normalized_time = time_series.map(_normalize_hhmm)
    result = pd.Series(pd.NaT, index=date_series.index, dtype="datetime64[ns]")

    with_time = date_values.notna() & normalized_time.notna()
    if with_time.any():
        result.loc[with_time] = pd.to_datetime(
            date_values.loc[with_time].dt.strftime("%Y-%m-%d") + " " + normalized_time.loc[with_time],
            errors="coerce",
        )

    without_time = date_values.notna() & normalized_time.isna()
    if without_time.any():
        result.loc[without_time] = date_values.loc[without_time]

    return result


def _build_reclamo_id(numero_tramite: pd.Series, numero_orden: pd.Series) -> pd.Series:
    return numero_tramite.astype("Int64").astype(str) + "-" + numero_orden.astype("Int64").astype(str)


def _prepare_tramites(tramites: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, int]]:
    work = tramites.copy()
    work["row_key"] = [f"tramites:{index}" for index in work.index]
    work["numero_tramite"] = pd.to_numeric(work["NumeroTramite"], errors="coerce").astype("Int64")
    work["numero_orden"] = pd.to_numeric(work["NumeroOrden"], errors="coerce").astype("Int64")
    work["reclamo_id"] = _build_reclamo_id(work["numero_tramite"], work["numero_orden"])

    work["fecha_reclamo"] = pd.to_datetime(work["dt_inicio"], errors="coerce")
    work["fecha_reclamo_alt"] = _combine_date_and_hhmm(work["fechainicio"], work["horainicio"])
    work["temporal_source"] = "dt_inicio"
    work["temporal_mismatch_minutes"] = (
        (work["fecha_reclamo"] - work["fecha_reclamo_alt"]).dt.total_seconds().abs().div(60)
    )

    work["geo1_num"] = pd.to_numeric(work["geo1"], errors="coerce")
    work["geo2_num"] = pd.to_numeric(work["geo2"], errors="coerce")

    direct_mask = work["geo1_num"].between(*LAT_BOUNDS) & work["geo2_num"].between(*LON_BOUNDS)
    swapped_mask = work["geo2_num"].between(*LAT_BOUNDS) & work["geo1_num"].between(*LON_BOUNDS)

    work["lat"] = pd.NA
    work["lon"] = pd.NA
    work.loc[direct_mask, "lat"] = work.loc[direct_mask, "geo1_num"]
    work.loc[direct_mask, "lon"] = work.loc[direct_mask, "geo2_num"]
    work.loc[swapped_mask, "lat"] = work.loc[swapped_mask, "geo2_num"]
    work.loc[swapped_mask, "lon"] = work.loc[swapped_mask, "geo1_num"]

    work["estado_geo"] = "invalid"
    work.loc[direct_mask, "estado_geo"] = "direct"
    work.loc[swapped_mask, "estado_geo"] = "swapped"

    exclusion_reason = pd.Series(pd.NA, index=work.index, dtype="object")
    exclusion_detail = pd.Series(pd.NA, index=work.index, dtype="object")

    invalid_date_mask = work["fecha_reclamo"].isna()
    exclusion_reason.loc[invalid_date_mask] = "fecha_canonica_invalida"
    exclusion_detail.loc[invalid_date_mask] = "dt_inicio no pudo parsearse"

    out_of_range_mask = work["fecha_reclamo"].notna() & ~work["fecha_reclamo"].between(
        DATE_WINDOW_START,
        DATE_WINDOW_END,
        inclusive="both",
    )
    exclusion_reason.loc[exclusion_reason.isna() & out_of_range_mask] = "fecha_fuera_rango_lluvias"
    exclusion_detail.loc[exclusion_reason == "fecha_fuera_rango_lluvias"] = (
        "Regla explícita: conservar solo reclamos entre 2021-01-01 y 2026-03-31 inclusive"
    )

    missing_geo_mask = work["geo1_num"].isna() | work["geo2_num"].isna()
    exclusion_reason.loc[exclusion_reason.isna() & missing_geo_mask] = "geo_missing_or_non_numeric"
    exclusion_detail.loc[exclusion_reason == "geo_missing_or_non_numeric"] = (
        "geo1/geo2 faltantes o no numéricos después de coerción"
    )

    zero_geo_mask = (work["geo1_num"] == 0) | (work["geo2_num"] == 0)
    exclusion_reason.loc[exclusion_reason.isna() & zero_geo_mask] = "geo_zero_placeholder"
    exclusion_detail.loc[exclusion_reason == "geo_zero_placeholder"] = "Coordenadas cero detectadas"

    invalid_bounds_mask = ~(direct_mask | swapped_mask)
    exclusion_reason.loc[exclusion_reason.isna() & invalid_bounds_mask] = "geo_out_of_bounds"
    exclusion_detail.loc[exclusion_reason == "geo_out_of_bounds"] = (
        f"Bounds válidos: lat {LAT_BOUNDS[0]}..{LAT_BOUNDS[1]}, lon {LON_BOUNDS[0]}..{LON_BOUNDS[1]}"
    )

    duplicate_mask = exclusion_reason.isna() & work.duplicated(subset=["reclamo_id"], keep="first")
    exclusion_reason.loc[duplicate_mask] = "duplicate_reclamo_id"
    exclusion_detail.loc[duplicate_mask] = "Se conserva la primera aparición del reclamo_id compuesto"

    clean = work.loc[exclusion_reason.isna()].copy()
    clean["lat"] = clean["lat"].astype(float)
    clean["lon"] = clean["lon"].astype(float)
    clean["motivo"] = clean["descrmotivo"].fillna("SIN_MOTIVO")
    clean["servicio"] = clean["serdes"].fillna("SIN_SERVICIO")
    clean["direccion"] = clean["dirdes"].fillna("SIN_DIRECCION")
    clean["localidad"] = clean["posloc"].fillna("SIN_LOCALIDAD")

    clean = clean[
        [
            "reclamo_id",
            "numero_tramite",
            "numero_orden",
            "fecha_reclamo",
            "fecha_reclamo_alt",
            "temporal_source",
            "lat",
            "lon",
            "estado_geo",
            "motivo",
            "servicio",
            "direccion",
            "localidad",
            "descrtipo",
            "codigomotivo",
        ]
    ].sort_values(["fecha_reclamo", "reclamo_id"], kind="stable")

    exclusions = work.loc[exclusion_reason.notna()].copy()
    exclusions["dataset"] = "tramites"
    exclusions["motivo_exclusion"] = exclusion_reason.loc[exclusion_reason.notna()]
    exclusions["detalle_exclusion"] = exclusion_detail.loc[exclusion_reason.notna()]
    exclusions = exclusions[
        [
            "row_key",
            "dataset",
            "reclamo_id",
            "numero_tramite",
            "numero_orden",
            "motivo_exclusion",
            "detalle_exclusion",
            "fecha_reclamo",
            "fecha_reclamo_alt",
            "geo1",
            "geo2",
            "estado_geo",
        ]
    ].sort_values(["motivo_exclusion", "row_key"], kind="stable")

    temporal_audit = work.loc[
        work["fecha_reclamo_alt"].notna() & (work["fecha_reclamo"] != work["fecha_reclamo_alt"]),
        ["row_key", "reclamo_id", "fecha_reclamo", "fecha_reclamo_alt", "temporal_mismatch_minutes"],
    ].sort_values("temporal_mismatch_minutes", ascending=False, kind="stable")

    summary = {
        "tramites_total": int(len(work)),
        "tramites_clean": int(len(clean)),
        "tramites_excluded": int(len(exclusions)),
        "tramites_temporal_mismatch": int(len(temporal_audit)),
        "tramites_geo_swapped": int((clean["estado_geo"] == "swapped").sum()),
    }
    return clean, exclusions, temporal_audit, summary


def _prepare_tareas(tareas: pd.DataFrame, reclamos_clean: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    work = tareas.copy()
    work["row_key"] = [f"tramites_tareas:{index}" for index in work.index]
    work["numero_tramite"] = pd.to_numeric(work["NumeroTramite"], errors="coerce").astype("Int64")
    work["numero_orden"] = pd.to_numeric(work["NumeroOrden"], errors="coerce").astype("Int64")
    work["reclamo_id"] = _build_reclamo_id(work["numero_tramite"], work["numero_orden"])
    work["fecha_tarea"] = _combine_date_and_hhmm(work["fecha"], work["hora"])
    work["tarea_id"] = work["codigotar"].astype("Int64").astype(str)
    work["tipo_tarea"] = work["tarea"].astype(str).str.strip()

    linked_reclamos = reclamos_clean[["reclamo_id"]].drop_duplicates().assign(_linked=True)
    work = work.merge(linked_reclamos, on="reclamo_id", how="left")

    exclusion_reason = pd.Series(pd.NA, index=work.index, dtype="object")
    exclusion_detail = pd.Series(pd.NA, index=work.index, dtype="object")

    invalid_date_mask = work["fecha_tarea"].isna()
    exclusion_reason.loc[invalid_date_mask] = "fecha_tarea_invalida"
    exclusion_detail.loc[invalid_date_mask] = "fecha/hora de tarea no pudo parsearse"

    orphan_mask = work["_linked"].isna()
    exclusion_reason.loc[exclusion_reason.isna() & orphan_mask] = "tarea_huerfana_sin_reclamo_clean"
    exclusion_detail.loc[exclusion_reason == "tarea_huerfana_sin_reclamo_clean"] = (
        "La tarea no encuentra reclamo retenido tras limpieza temporal y geográfica"
    )

    clean = work.loc[exclusion_reason.isna()].copy()
    clean = clean[
        [
            "reclamo_id",
            "tarea_id",
            "fecha_tarea",
            "tipo_tarea",
            "numero_tramite",
            "numero_orden",
            "hora",
        ]
    ].sort_values(["fecha_tarea", "reclamo_id", "tarea_id"], kind="stable")

    exclusions = work.loc[exclusion_reason.notna()].copy()
    exclusions["dataset"] = "tramites_tareas"
    exclusions["motivo_exclusion"] = exclusion_reason.loc[exclusion_reason.notna()]
    exclusions["detalle_exclusion"] = exclusion_detail.loc[exclusion_reason.notna()]
    exclusions = exclusions[
        [
            "row_key",
            "dataset",
            "reclamo_id",
            "numero_tramite",
            "numero_orden",
            "motivo_exclusion",
            "detalle_exclusion",
            "fecha_tarea",
            "tarea_id",
            "tipo_tarea",
        ]
    ].sort_values(["motivo_exclusion", "row_key"], kind="stable")

    summary = {
        "tareas_total": int(len(work)),
        "tareas_clean": int(len(clean)),
        "tareas_excluded": int(len(exclusions)),
    }
    return clean, exclusions, summary


def _build_destinos_unicos(reclamos_clean: pd.DataFrame) -> pd.DataFrame:
    destinations = reclamos_clean.copy()
    destinations["lat"] = destinations["lat"].round(5)
    destinations["lon"] = destinations["lon"].round(5)
    destinations["destino_key"] = destinations["lat"].map(lambda value: f"{value:.5f}") + "_" + destinations["lon"].map(
        lambda value: f"{value:.5f}"
    )

    grouped = (
        destinations.groupby(["destino_key", "lat", "lon"], dropna=False)
        .agg(reclamos_count=("reclamo_id", "nunique"))
        .reset_index()
        .sort_values(["reclamos_count", "destino_key"], ascending=[False, True], kind="stable")
    )
    return grouped


def _write_phase2_report(
    report_path: Path,
    summary: dict[str, int],
    reclamos_exclusions: pd.DataFrame,
    tareas_exclusions: pd.DataFrame,
    temporal_audit: pd.DataFrame,
    destinos_unicos: pd.DataFrame,
) -> None:
    reclamos_reason = reclamos_exclusions["motivo_exclusion"].value_counts().sort_index()
    tareas_reason = tareas_exclusions["motivo_exclusion"].value_counts().sort_index()

    report_lines = [
        "# Auditoría de depuración — Phase 2",
        "",
        "## Regla temporal canónica de `tramites`",
        "",
        "- Fuente temporal canónica: `dt_inicio`.",
        "- Regla explícita: conservar solo reclamos con `dt_inicio` entre `2021-01-01 00:00:00` y `2026-03-31 23:59:59` inclusive.",
        f"- Auditoría temporal: `{len(temporal_audit)}` filas difieren contra `fechainicio` + `horainicio`; se conserva `dt_inicio` por consistencia y cobertura completa.",
        "",
        "## Regla geográfica",
        "",
        f"- Bounds válidos: latitud `{LAT_BOUNDS[0]}` a `{LAT_BOUNDS[1]}`, longitud `{LON_BOUNDS[0]}` a `{LON_BOUNDS[1]}`.",
        f"- Se detectaron `{summary['tramites_geo_swapped']}` reclamos con columnas `geo1`/`geo2` invertidas y se corrigieron antes de persistir `lat`/`lon`.",
        "",
        "## Resultados",
        "",
        f"- Reclamos totales analizados: `{summary['tramites_total']}`.",
        f"- Reclamos retenidos en `data/processed/reclamos_clean.parquet`: `{summary['tramites_clean']}`.",
        f"- Reclamos excluidos en `data/intermediate/exclusiones_reclamos.parquet`: `{summary['tramites_excluded']}`.",
        f"- Tareas retenidas en `data/processed/tareas_clean.parquet`: `{summary['tareas_clean']}`.",
        f"- Tareas excluidas en `data/intermediate/exclusiones_tareas.parquet`: `{summary['tareas_excluded']}`.",
        f"- Destinos únicos para ruteo en `data/processed/destinos_unicos.parquet`: `{len(destinos_unicos)}`.",
        "",
        "## Exclusiones de reclamos por motivo",
        "",
    ]

    for reason, count in reclamos_reason.items():
        report_lines.append(f"- `{reason}`: `{int(count)}`")

    report_lines.extend(["", "## Exclusiones de tareas por motivo", ""])
    for reason, count in tareas_reason.items():
        report_lines.append(f"- `{reason}`: `{int(count)}`")

    report_lines.extend(
        [
            "",
            "## Mapeo reclamo ↔ tarea",
            "",
            "- Clave de enlace retenida: `reclamo_id = NumeroTramite-NumeroOrden`.",
            "- En tareas se mapeó `codigotar -> tarea_id`, `fecha/hora -> fecha_tarea` y `tarea -> tipo_tarea`.",
            "- Solo se retienen tareas cuyo `reclamo_id` existe en `reclamos_clean`.",
            "",
            "## Outputs generados",
            "",
            "- `data/intermediate/exclusiones_reclamos.parquet`",
            "- `data/intermediate/exclusiones_tareas.parquet`",
            "- `data/intermediate/tramites_temporal_audit.parquet`",
            "- `data/processed/reclamos_clean.parquet`",
            "- `data/processed/tareas_clean.parquet`",
            "- `data/processed/destinos_unicos.parquet`",
        ]
    )

    report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")


def run_phase2_pipeline() -> dict[str, object]:
    ensure_workspace_layout()

    tramites_path = require_input("tramites")
    tareas_path = require_input("tramites_tareas")

    tramites = pd.read_parquet(tramites_path)
    tareas = pd.read_parquet(tareas_path)
    validate_raw_inputs(tramites, tareas)

    reclamos_clean, exclusiones_reclamos, temporal_audit, reclamos_summary = _prepare_tramites(tramites)
    tareas_clean, exclusiones_tareas, tareas_summary = _prepare_tareas(tareas, reclamos_clean)
    destinos_unicos = _build_destinos_unicos(reclamos_clean)

    validate_processed_contract(reclamos_clean, "reclamos_clean")
    validate_processed_contract(tareas_clean, "tareas_clean")
    validate_processed_contract(destinos_unicos, "destinos_unicos")

    artifacts = Phase2Artifacts(
        reclamos_clean=PROCESSED_DIR / "reclamos_clean.parquet",
        tareas_clean=PROCESSED_DIR / "tareas_clean.parquet",
        destinos_unicos=PROCESSED_DIR / "destinos_unicos.parquet",
        exclusiones_reclamos=INTERMEDIATE_DIR / "exclusiones_reclamos.parquet",
        exclusiones_tareas=INTERMEDIATE_DIR / "exclusiones_tareas.parquet",
        tramites_temporal_audit=INTERMEDIATE_DIR / "tramites_temporal_audit.parquet",
        report=REPORTS_DIR / "phase2_cleaning_audit.md",
    )

    reclamos_clean.to_parquet(artifacts.reclamos_clean, index=False)
    tareas_clean.to_parquet(artifacts.tareas_clean, index=False)
    destinos_unicos.to_parquet(artifacts.destinos_unicos, index=False)
    exclusiones_reclamos.to_parquet(artifacts.exclusiones_reclamos, index=False)
    exclusiones_tareas.to_parquet(artifacts.exclusiones_tareas, index=False)
    temporal_audit.to_parquet(artifacts.tramites_temporal_audit, index=False)

    summary = {**reclamos_summary, **tareas_summary, "destinos_unicos": int(len(destinos_unicos))}
    _write_phase2_report(
        report_path=artifacts.report,
        summary=summary,
        reclamos_exclusions=exclusiones_reclamos,
        tareas_exclusions=exclusiones_tareas,
        temporal_audit=temporal_audit,
        destinos_unicos=destinos_unicos,
    )

    return {
        "summary": summary,
        "artifacts": {field: str(getattr(artifacts, field)) for field in artifacts.__dataclass_fields__},
        "reclamos_exclusion_counts": exclusiones_reclamos["motivo_exclusion"].value_counts().sort_index().to_dict(),
        "tareas_exclusion_counts": exclusiones_tareas["motivo_exclusion"].value_counts().sort_index().to_dict(),
        "temporal_mismatch_rows": int(len(temporal_audit)),
    }


if __name__ == "__main__":
    result = run_phase2_pipeline()
    print(pd.Series(result["summary"]).to_string())
