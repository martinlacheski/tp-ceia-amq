from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from helpers.contracts import validate_processed_contract
from helpers.operational_refs import (
    ENERGIA_SEDE_ID,
    assign_sede_by_service,
    build_destino_key,
    load_observed_service_sede_ref,
    load_sede_ref,
)
from helpers.paths import INTERMEDIATE_DIR, PROCESSED_DIR, REPORTS_DIR, ensure_workspace_layout, require_input


@dataclass(slots=True)
class ReferenceArtifacts:
    sede_ref: Path
    servicio_sede_ref: Path
    costos_ref: Path
    report: Path


@dataclass(slots=True)
class CostingArtifacts:
    distancias_costos: Path
    reclamos_enriquecidos: Path
    report: Path


@dataclass(slots=True)
class AggregateArtifacts:
    resumen_operativo_diario: Path
    resumen_hotspots: Path
    report: Path


def _load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _build_costos_ref() -> pd.DataFrame:
    costos_path = require_input("costos_vehiculos")
    tarifas_path = require_input("tarifas_ceml")
    costos_payload = _load_json(costos_path)
    tarifas_payload = _load_json(tarifas_path)

    costo_km = float(costos_payload["vehiculo_promedio_flota"]["costo_por_km_ars"])
    costo_hora = float(tarifas_payload["mano_de_obra"]["hora_hombre_normal"])

    costos_ref = pd.DataFrame(
        [
            {
                "vehiculo_tipo": "vehiculo_promedio_flota",
                "costo_km": costo_km,
                "costo_hora": costo_hora,
                "combustible_tipo": costos_payload["combustible"]["tipo"],
                "combustible_precio_litro_ars": float(costos_payload["combustible"]["precio_por_litro_ars"]),
                "rendimiento_km_litro": float(costos_payload["vehiculo_promedio_flota"]["rendimiento_km_por_litro"]),
                "tarifa_version": tarifas_payload["version"],
            }
        ]
    )
    validate_processed_contract(costos_ref[["vehiculo_tipo", "costo_km", "costo_hora"]], "costos_ref")
    return costos_ref


def _write_report(path: Path, sede_ref: pd.DataFrame, servicio_sede_ref: pd.DataFrame, costos_ref: pd.DataFrame) -> None:
    costos = costos_ref.iloc[0]
    lines = [
        "# Referencias operativas — preparación Phase 3",
        "",
        "## Sedes operativas",
        "",
    ]
    for _, sede in sede_ref.sort_values("sede_id", kind="stable").iterrows():
        lines.extend(
            [
                f"- `sede_id`: `{sede['sede_id']}` | `sede_nombre`: `{sede['sede_nombre']}` | coordenadas `{sede['lat']}`, `{sede['lon']}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Mapeo observado servicio → sede",
            "",
        ]
    )
    for _, row in servicio_sede_ref.sort_values(["sede_id", "servicio_normalizado"], kind="stable").iterrows():
        lines.append(
            f"- `{row['servicio']}` → `{row['sede_id']}` (`{row['sede_nombre']}`) vía `{row['mapping_source']}`"
        )
    lines.extend(
        [
            "",
        "## Costos base",
        "",
        f"- `vehiculo_tipo`: `{costos['vehiculo_tipo']}`",
        f"- `costo_km`: `{costos['costo_km']}` ARS/km",
        f"- `costo_hora`: `{costos['costo_hora']}` ARS/hora",
        f"- `combustible_tipo`: `{costos['combustible_tipo']}`",
        f"- `combustible_precio_litro_ars`: `{costos['combustible_precio_litro_ars']}`",
        f"- `rendimiento_km_litro`: `{costos['rendimiento_km_litro']}`",
        "",
        "## Outputs generados",
        "",
        "- `data/processed/sede_ref.parquet`",
        "- `data/processed/servicio_sede_ref.parquet`",
        "- `data/processed/costos_ref.parquet`",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _load_latest_cache() -> pd.DataFrame:
    cache_path = INTERMEDIATE_DIR / "distancias_cache.parquet"
    cache = pd.read_parquet(cache_path)
    if cache.empty:
        raise ValueError("distancias_cache.parquet está vacío; no hay cache para expandir a reclamos")
    cache = cache.sort_values(["requested_at", "sede_id", "destino_key"], kind="stable")
    cache = cache.drop_duplicates(subset=["sede_id", "destino_key"], keep="last").reset_index(drop=True)
    return cache


def _load_single_row_reference(path: Path, dataset_name: str) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    if len(frame) != 1:
        raise ValueError(f"{dataset_name} debe tener exactamente una fila y encontró {len(frame)}")
    return frame


def _compute_costing_outputs() -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int | float]]:
    reclamos = pd.read_parquet(PROCESSED_DIR / "reclamos_clean.parquet").copy()
    reclamos = assign_sede_by_service(reclamos)
    reclamos["destino_key"] = build_destino_key(reclamos["lat"], reclamos["lon"])

    cache = _load_latest_cache()
    sede_ref = pd.read_parquet(PROCESSED_DIR / "sede_ref.parquet")
    validate_processed_contract(sede_ref[["sede_id", "sede_nombre", "lat", "lon"]], "sede_ref")
    costos_ref = _load_single_row_reference(PROCESSED_DIR / "costos_ref.parquet", "costos_ref")

    cache_columns = [
        "sede_id",
        "destino_key",
        "distance_m",
        "duration_s",
        "routing_status",
        "fuente_ruteo",
        "api_status",
        "error_message",
        "requested_at",
        "cache_version",
    ]
    enriched = reclamos.merge(cache[cache_columns], on=["sede_id", "destino_key"], how="left", validate="many_to_one")
    enriched["routing_status"] = enriched["routing_status"].fillna("pending")
    enriched["fuente_ruteo"] = enriched["fuente_ruteo"].fillna("google_maps_directions")
    enriched["cache_version"] = enriched["cache_version"].fillna("v1")
    enriched["api_status"] = enriched["api_status"].fillna("NOT_REQUESTED")
    enriched = enriched.merge(
        sede_ref[["sede_id", "sede_nombre", "lat", "lon"]].rename(columns={"lat": "sede_lat_ref", "lon": "sede_lon_ref"}),
        on=["sede_id", "sede_nombre"],
        how="left",
        validate="many_to_one",
    )

    enriched["distance_km"] = pd.NA
    enriched["duration_h"] = pd.NA
    enriched["costo_km_ars"] = pd.NA
    enriched["costo_hora_ars"] = pd.NA
    enriched["costo_operativo_ars"] = pd.NA
    enriched["routing_available"] = enriched["routing_status"].eq("ok")
    enriched["routing_exclusion_reason"] = pd.NA

    ok_mask = enriched["routing_available"]
    costo_km_base = float(costos_ref.iloc[0]["costo_km"])
    costo_hora_base = float(costos_ref.iloc[0]["costo_hora"])
    if ok_mask.any():
        enriched.loc[ok_mask, "distance_km"] = enriched.loc[ok_mask, "distance_m"].astype(float).div(1000)
        enriched.loc[ok_mask, "duration_h"] = enriched.loc[ok_mask, "duration_s"].astype(float).div(3600)
        enriched.loc[ok_mask, "costo_km_ars"] = enriched.loc[ok_mask, "distance_km"].astype(float).mul(costo_km_base)
        enriched.loc[ok_mask, "costo_hora_ars"] = enriched.loc[ok_mask, "duration_h"].astype(float).mul(costo_hora_base)
        enriched.loc[ok_mask, "costo_operativo_ars"] = (
            enriched.loc[ok_mask, "costo_km_ars"].astype(float) + enriched.loc[ok_mask, "costo_hora_ars"].astype(float)
        )

    non_ok_mask = ~ok_mask
    enriched.loc[non_ok_mask, "routing_exclusion_reason"] = "routing_" + enriched.loc[non_ok_mask, "routing_status"].astype(str)

    enriched["vehiculo_tipo"] = str(costos_ref.iloc[0]["vehiculo_tipo"])
    enriched["costo_km_base_ars"] = costo_km_base
    enriched["costo_hora_base_ars"] = costo_hora_base
    enriched["tarifa_version"] = str(costos_ref.iloc[0]["tarifa_version"])

    distancias_costos = enriched.loc[ok_mask].copy()
    distancias_costos = distancias_costos[
        [
            "reclamo_id",
            "fecha_reclamo",
            "destino_key",
            "sede_id",
            "sede_nombre",
            "distance_m",
            "duration_s",
            "distance_km",
            "duration_h",
            "costo_km_base_ars",
            "costo_hora_base_ars",
            "costo_km_ars",
            "costo_hora_ars",
            "costo_operativo_ars",
            "routing_status",
            "fuente_ruteo",
            "requested_at",
            "cache_version",
            "vehiculo_tipo",
            "tarifa_version",
        ]
    ].sort_values(["fecha_reclamo", "reclamo_id"], kind="stable")

    reclamos_enriquecidos = enriched[
        [
            "reclamo_id",
            "numero_tramite",
            "numero_orden",
            "fecha_reclamo",
            "lat",
            "lon",
            "estado_geo",
            "motivo",
            "servicio",
            "servicio_normalizado",
            "direccion",
            "localidad",
            "destino_key",
            "sede_id",
            "sede_nombre",
            "sede_lat",
            "sede_lon",
            "routing_status",
            "routing_available",
            "routing_exclusion_reason",
            "distance_m",
            "duration_s",
            "distance_km",
            "duration_h",
            "costo_km_ars",
            "costo_hora_ars",
            "costo_operativo_ars",
            "fuente_ruteo",
            "api_status",
            "error_message",
            "requested_at",
            "cache_version",
            "vehiculo_tipo",
            "costo_km_base_ars",
            "costo_hora_base_ars",
            "tarifa_version",
        ]
    ].sort_values(["fecha_reclamo", "reclamo_id"], kind="stable")

    for numeric_column in [
        "distance_m",
        "duration_s",
        "distance_km",
        "duration_h",
        "costo_km_ars",
        "costo_hora_ars",
        "costo_operativo_ars",
        "costo_km_base_ars",
        "costo_hora_base_ars",
    ]:
        reclamos_enriquecidos[numeric_column] = pd.to_numeric(reclamos_enriquecidos[numeric_column], errors="coerce")
        if numeric_column in distancias_costos.columns:
            distancias_costos[numeric_column] = pd.to_numeric(distancias_costos[numeric_column], errors="coerce")

    validate_processed_contract(distancias_costos, "distancias_costos")
    validate_processed_contract(reclamos_enriquecidos, "reclamos_enriquecidos")

    summary = {
        "reclamos_clean_total": int(len(reclamos)),
        "reclamos_enriquecidos_total": int(len(reclamos_enriquecidos)),
        "reclamos_costeados_ok": int(len(distancias_costos)),
        "destinos_ok_cache": int(cache["routing_status"].eq("ok").sum()),
        "destinos_pending_cache": int(cache["routing_status"].eq("pending").sum()),
        "routing_non_ok_reclamos": int((~reclamos_enriquecidos["routing_available"]).sum()),
        "costo_operativo_total_ars": float(distancias_costos["costo_operativo_ars"].sum()) if not distancias_costos.empty else 0.0,
        "sedes_operativas": int(reclamos_enriquecidos["sede_id"].nunique()),
    }
    return distancias_costos, reclamos_enriquecidos, summary


def _write_costing_report(path: Path, summary: dict[str, int | float], reclamos_enriquecidos: pd.DataFrame) -> None:
    status_counts = reclamos_enriquecidos["routing_status"].value_counts(dropna=False).sort_index()
    coverage_by_sede = (
        reclamos_enriquecidos.groupby(["sede_id", "sede_nombre"], dropna=False)
        .agg(
            reclamos_total=("reclamo_id", "size"),
            reclamos_ok=("routing_available", "sum"),
            costo_operativo_total_ars=("costo_operativo_ars", "sum"),
            servicios_unicos=("servicio_normalizado", "nunique"),
        )
        .reset_index()
        .sort_values(["reclamos_total", "sede_id"], ascending=[False, True], kind="stable")
    )
    coverage_by_service = (
        reclamos_enriquecidos.groupby(["sede_id", "servicio"], dropna=False)
        .agg(
            reclamos_total=("reclamo_id", "size"),
            reclamos_ok=("routing_available", "sum"),
            costo_operativo_total_ars=("costo_operativo_ars", "sum"),
        )
        .reset_index()
        .sort_values(["sede_id", "reclamos_total", "servicio"], ascending=[True, False, True], kind="stable")
    )
    lines = [
        "# Costeo operativo inicial — Phase 3.3",
        "",
        "## Objetivo",
        "",
        "- Expandir el cache de rutas en tandas controladas y traducir la cobertura actual a un dataset de costo operativo por reclamo.",
        "- Mantener `reclamos_enriquecidos.parquet` como vista completa del universo limpio, dejando `distancias_costos.parquet` solo con reclamos costeables (`routing_status = ok`).",
        "",
        "## Resultados",
        "",
        f"- Reclamos limpios totales: `{summary['reclamos_clean_total']}`.",
        f"- Reclamos enriquecidos totales: `{summary['reclamos_enriquecidos_total']}`.",
        f"- Reclamos con costo operativo disponible: `{summary['reclamos_costeados_ok']}`.",
        f"- Destinos `ok` actualmente en cache: `{summary['destinos_ok_cache']}`.",
        f"- Destinos `pending` actualmente en cache: `{summary['destinos_pending_cache']}`.",
        f"- Reclamos aún no costeables por falta de ruta OK: `{summary['routing_non_ok_reclamos']}`.",
        f"- Costo operativo total cubierto por la tanda actual: `{summary['costo_operativo_total_ars']:.2f}` ARS.",
        f"- Sedes operativas cubiertas: `{summary['sedes_operativas']}`.",
        "",
        "## Estados de ruteo en `reclamos_enriquecidos.parquet`",
        "",
    ]
    for status, count in status_counts.items():
        lines.append(f"- `{status}`: `{int(count)}` reclamos")

    lines.extend(["", "## Cobertura por sede", ""])
    for _, row in coverage_by_sede.iterrows():
        lines.append(
            "- `{sede}` ({nombre}): reclamos=`{total}`, ok=`{ok}`, servicios=`{services}`, costo_total=`{cost:.2f}` ARS".format(
                sede=row["sede_id"],
                nombre=row["sede_nombre"],
                total=int(row["reclamos_total"]),
                ok=int(row["reclamos_ok"]),
                services=int(row["servicios_unicos"]),
                cost=float(row["costo_operativo_total_ars"]),
            )
        )

    lines.extend(["", "## Cobertura por servicio", ""])
    for _, row in coverage_by_service.iterrows():
        lines.append(
            "- `{servicio}` → `{sede}`: reclamos=`{total}`, ok=`{ok}`, costo_total=`{cost:.2f}` ARS".format(
                servicio=row["servicio"],
                sede=row["sede_id"],
                total=int(row["reclamos_total"]),
                ok=int(row["reclamos_ok"]),
                cost=float(row["costo_operativo_total_ars"]),
            )
        )

    lines.extend(
        [
            "",
            "## Outputs generados",
            "",
            "- `data/processed/distancias_costos.parquet`",
            "- `data/processed/reclamos_enriquecidos.parquet`",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _mode_or_first(series: pd.Series) -> str:
    cleaned = series.dropna().astype(str).str.strip()
    cleaned = cleaned[cleaned != ""]
    if cleaned.empty:
        return ""
    modes = cleaned.mode()
    if not modes.empty:
        return str(modes.iloc[0])
    return str(cleaned.iloc[0])


def _build_operational_daily_summary(
    reclamos_enriquecidos: pd.DataFrame,
    lluvia_diaria: pd.DataFrame,
) -> pd.DataFrame:
    frame = reclamos_enriquecidos.copy()
    frame["fecha"] = pd.to_datetime(frame["fecha_reclamo"], errors="coerce").dt.normalize()
    frame = frame.loc[frame["fecha"].notna()].copy()
    frame["routing_available"] = frame["routing_available"].fillna(False).astype(bool)

    for column in ["distance_m", "distance_km", "duration_s", "duration_h", "costo_operativo_ars"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    for source_column, target_column in [
        ("distance_m", "distance_m_ok"),
        ("distance_km", "distance_km_ok"),
        ("duration_s", "duration_s_ok"),
        ("duration_h", "duration_h_ok"),
        ("costo_operativo_ars", "costo_operativo_ok_ars"),
    ]:
        frame[target_column] = frame[source_column].where(frame["routing_available"], 0.0).fillna(0.0)

    daily = (
        frame.groupby("fecha", dropna=False)
        .agg(
            reclamos_total=("reclamo_id", "size"),
            reclamos_ruteados_ok=("routing_available", "sum"),
            destinos_unicos=("destino_key", "nunique"),
            localidades_unicas=("localidad", "nunique"),
            motivos_unicos=("motivo", "nunique"),
            distance_m_total=("distance_m_ok", "sum"),
            distance_km_total=("distance_km_ok", "sum"),
            duration_s_total=("duration_s_ok", "sum"),
            duration_h_total=("duration_h_ok", "sum"),
            costo_operativo_total_ars=("costo_operativo_ok_ars", "sum"),
        )
        .reset_index()
        .sort_values("fecha", kind="stable")
    )
    daily["reclamos_pendientes_ruteo"] = daily["reclamos_total"] - daily["reclamos_ruteados_ok"]
    daily["cobertura_ruteo_pct"] = (
        daily["reclamos_ruteados_ok"].div(daily["reclamos_total"].where(daily["reclamos_total"] > 0)).fillna(0.0) * 100.0
    ).round(2)
    daily["costo_operativo_promedio_ars"] = (
        daily["costo_operativo_total_ars"].div(daily["reclamos_ruteados_ok"].where(daily["reclamos_ruteados_ok"] > 0)).fillna(0.0)
    ).round(2)

    lluvia = lluvia_diaria[["fecha", "lluvia_mm", "lluvia_status"]].drop_duplicates(subset=["fecha"], keep="last")
    daily = daily.merge(lluvia, on="fecha", how="left")
    daily["lluvia_status"] = daily["lluvia_status"].fillna("out_of_range")
    validate_processed_contract(daily, "resumen_operativo_diario")
    return daily


def _build_hotspots_summary(reclamos_enriquecidos: pd.DataFrame) -> pd.DataFrame:
    frame = reclamos_enriquecidos.copy()
    frame["routing_available"] = frame["routing_available"].fillna(False).astype(bool)
    for column in ["distance_km", "duration_h", "costo_operativo_ars"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame[column] = frame[column].where(frame["routing_available"], 0.0).fillna(0.0)

    hotspots = (
        frame.groupby(["sede_id", "sede_nombre", "destino_key", "lat", "lon"], dropna=False)
        .agg(
            reclamos_total=("reclamo_id", "size"),
            reclamos_ruteados_ok=("routing_available", "sum"),
            localidades_unicas=("localidad", "nunique"),
            motivos_unicos=("motivo", "nunique"),
            localidad_referencia=("localidad", _mode_or_first),
            direccion_referencia=("direccion", _mode_or_first),
            motivo_referencia=("motivo", _mode_or_first),
            primer_reclamo_fecha=("fecha_reclamo", "min"),
            ultimo_reclamo_fecha=("fecha_reclamo", "max"),
            distance_km_total=("distance_km", "sum"),
            duration_h_total=("duration_h", "sum"),
            costo_operativo_total_ars=("costo_operativo_ars", "sum"),
        )
        .reset_index()
    )
    hotspots["reclamos_pendientes_ruteo"] = hotspots["reclamos_total"] - hotspots["reclamos_ruteados_ok"]
    hotspots["cobertura_ruteo_pct"] = (
        hotspots["reclamos_ruteados_ok"].div(hotspots["reclamos_total"].where(hotspots["reclamos_total"] > 0)).fillna(0.0) * 100.0
    ).round(2)
    hotspots["costo_operativo_promedio_ars"] = (
        hotspots["costo_operativo_total_ars"]
        .div(hotspots["reclamos_ruteados_ok"].where(hotspots["reclamos_ruteados_ok"] > 0))
        .fillna(0.0)
        .round(2)
    )
    hotspots = hotspots.sort_values(
        ["costo_operativo_total_ars", "reclamos_total", "destino_key"],
        ascending=[False, False, True],
        kind="stable",
    ).reset_index(drop=True)
    validate_processed_contract(hotspots, "resumen_hotspots")
    return hotspots


def _write_aggregates_report(
    path: Path,
    resumen_operativo_diario: pd.DataFrame,
    resumen_hotspots: pd.DataFrame,
) -> None:
    top_hotspot = resumen_hotspots.iloc[0] if not resumen_hotspots.empty else None
    lines = [
        "# Agregados operativos mínimos — Phase 3.4",
        "",
        "## Objetivo",
        "",
        "- Resumir el universo enriquecido por fecha para habilitar argumentos de recurrencia, costo y cobertura.",
        "- Dejar un ranking de hotspots operativos reutilizable por el notebook final sin recalcular el costeo.",
        "",
        "## Resultados",
        "",
        f"- Días resumidos: `{len(resumen_operativo_diario)}`.",
        f"- Hotspots únicos: `{len(resumen_hotspots)}`.",
        f"- Costo operativo total resumido: `{resumen_operativo_diario['costo_operativo_total_ars'].sum():.2f}` ARS.",
        f"- Reclamos ruteados acumulados: `{int(resumen_operativo_diario['reclamos_ruteados_ok'].sum())}`.",
    ]
    if top_hotspot is not None:
        lines.extend(
            [
                "",
                "## Hotspot líder actual",
                "",
                f"- `sede_id`: `{top_hotspot['sede_id']}`.",
                f"- `sede_nombre`: `{top_hotspot['sede_nombre']}`.",
                f"- `destino_key`: `{top_hotspot['destino_key']}`.",
                f"- `localidad_referencia`: `{top_hotspot['localidad_referencia']}`.",
                f"- `direccion_referencia`: `{top_hotspot['direccion_referencia']}`.",
                f"- `reclamos_total`: `{int(top_hotspot['reclamos_total'])}`.",
                f"- `reclamos_ruteados_ok`: `{int(top_hotspot['reclamos_ruteados_ok'])}`.",
                f"- `costo_operativo_total_ars`: `{float(top_hotspot['costo_operativo_total_ars']):.2f}`.",
            ]
        )
    lines.extend(
        [
            "",
            "## Outputs generados",
            "",
            "- `data/processed/resumen_operativo_diario.parquet`",
            "- `data/processed/resumen_hotspots.parquet`",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_reference_outputs() -> dict[str, object]:
    ensure_workspace_layout()
    sede_ref = load_sede_ref()
    servicio_sede_ref = load_observed_service_sede_ref()
    costos_ref = _build_costos_ref()
    artifacts = ReferenceArtifacts(
        sede_ref=PROCESSED_DIR / "sede_ref.parquet",
        servicio_sede_ref=PROCESSED_DIR / "servicio_sede_ref.parquet",
        costos_ref=PROCESSED_DIR / "costos_ref.parquet",
        report=REPORTS_DIR / "phase3_reference_inputs.md",
    )

    sede_ref.to_parquet(artifacts.sede_ref, index=False)
    servicio_sede_ref.to_parquet(artifacts.servicio_sede_ref, index=False)
    costos_ref.to_parquet(artifacts.costos_ref, index=False)
    _write_report(artifacts.report, sede_ref=sede_ref, servicio_sede_ref=servicio_sede_ref, costos_ref=costos_ref)

    return {
        "summary": {
            "sede_rows": int(len(sede_ref)),
            "servicio_sede_rows": int(len(servicio_sede_ref)),
            "costos_rows": int(len(costos_ref)),
            "sedes": sede_ref["sede_id"].tolist(),
        },
        "artifacts": {field: str(getattr(artifacts, field)) for field in artifacts.__dataclass_fields__},
    }


def build_costing_outputs() -> dict[str, object]:
    ensure_workspace_layout()
    distancias_costos, reclamos_enriquecidos, summary = _compute_costing_outputs()
    artifacts = CostingArtifacts(
        distancias_costos=PROCESSED_DIR / "distancias_costos.parquet",
        reclamos_enriquecidos=PROCESSED_DIR / "reclamos_enriquecidos.parquet",
        report=REPORTS_DIR / "phase3_costing_outputs.md",
    )
    distancias_costos.to_parquet(artifacts.distancias_costos, index=False)
    reclamos_enriquecidos.to_parquet(artifacts.reclamos_enriquecidos, index=False)
    _write_costing_report(artifacts.report, summary=summary, reclamos_enriquecidos=reclamos_enriquecidos)
    return {
        "summary": summary,
        "artifacts": {field: str(getattr(artifacts, field)) for field in artifacts.__dataclass_fields__},
    }


def build_operational_aggregates() -> dict[str, object]:
    ensure_workspace_layout()
    reclamos_enriquecidos = pd.read_parquet(PROCESSED_DIR / "reclamos_enriquecidos.parquet")
    lluvia_diaria = pd.read_parquet(PROCESSED_DIR / "lluvia_diaria_clean.parquet")

    resumen_operativo_diario = _build_operational_daily_summary(reclamos_enriquecidos, lluvia_diaria)
    resumen_hotspots = _build_hotspots_summary(reclamos_enriquecidos)

    artifacts = AggregateArtifacts(
        resumen_operativo_diario=PROCESSED_DIR / "resumen_operativo_diario.parquet",
        resumen_hotspots=PROCESSED_DIR / "resumen_hotspots.parquet",
        report=REPORTS_DIR / "phase3_operational_aggregates.md",
    )
    resumen_operativo_diario.to_parquet(artifacts.resumen_operativo_diario, index=False)
    resumen_hotspots.to_parquet(artifacts.resumen_hotspots, index=False)
    _write_aggregates_report(artifacts.report, resumen_operativo_diario, resumen_hotspots)

    summary = {
        "dias_resumidos": int(len(resumen_operativo_diario)),
        "hotspots_unicos": int(len(resumen_hotspots)),
        "reclamos_ruteados_ok": int(resumen_operativo_diario["reclamos_ruteados_ok"].sum()),
        "costo_operativo_total_ars": float(resumen_operativo_diario["costo_operativo_total_ars"].sum()),
    }
    return {
        "summary": summary,
        "artifacts": {field: str(getattr(artifacts, field)) for field in artifacts.__dataclass_fields__},
    }


if __name__ == "__main__":
    print(pd.Series(write_reference_outputs()["summary"]).to_string())
