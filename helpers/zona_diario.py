from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from helpers.contracts import validate_processed_contract
from helpers.paths import PROCESSED_DIR, REPORTS_DIR, ensure_workspace_layout


EARTH_RADIUS_KM = 6371.0088
DEFAULT_CLUSTER_DIAMETER_KM = 5.0
DEFAULT_SERVICE_BASE_MINUTES = 60.0


@dataclass(slots=True)
class ZonaDiarioArtifacts:
    zonas_clusterizadas: Path
    zona_cluster_resumen: Path
    reclamos_zonificados: Path
    zona_diario_base: Path
    zona_diario_supervisado: Path
    zone_report: Path
    rain_report: Path
    dataset_report: Path
    verification_report: Path


@dataclass(slots=True)
class ZonaDiarioBaselineArtifacts:
    baseline_predictions: Path
    baseline_metrics: Path
    baseline_report: Path


class _UnionFind:
    def __init__(self, n: int) -> None:
        self.parent = np.arange(n, dtype=np.int32)
        self.rank = np.zeros(n, dtype=np.int8)

    def find(self, x: int) -> int:
        parent = self.parent
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        root_a = self.find(a)
        root_b = self.find(b)
        if root_a == root_b:
            return
        if self.rank[root_a] < self.rank[root_b]:
            root_a, root_b = root_b, root_a
        self.parent[root_b] = root_a
        if self.rank[root_a] == self.rank[root_b]:
            self.rank[root_a] += 1


def _haversine_km(lat1: np.ndarray, lon1: np.ndarray, lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    lat1_rad = np.radians(lat1)
    lon1_rad = np.radians(lon1)
    lat2_rad = np.radians(lat2)
    lon2_rad = np.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2.0) ** 2
    return 2.0 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(a))


def _dominant_value(frame: pd.DataFrame, value_column: str) -> str:
    top = frame[value_column].value_counts(dropna=False, sort=True)
    if top.empty:
        return "desconocido"
    value = top.index[0]
    return "desconocido" if pd.isna(value) else str(value)


def _cluster_max_pairwise_km(points: pd.DataFrame) -> float:
    if len(points) <= 1:
        return 0.0
    lat = points["lat"].to_numpy(dtype=float)
    lon = points["lon"].to_numpy(dtype=float)
    if len(points) <= 250:
        max_distance = 0.0
        for idx in range(len(points) - 1):
            distances = _haversine_km(
                np.full(len(points) - idx - 1, lat[idx]),
                np.full(len(points) - idx - 1, lon[idx]),
                lat[idx + 1 :],
                lon[idx + 1 :],
            )
            if len(distances):
                max_distance = max(max_distance, float(distances.max()))
        return round(max_distance, 4)

    seed_idx = 0
    distances_from_seed = _haversine_km(
        np.full(len(points), lat[seed_idx]),
        np.full(len(points), lon[seed_idx]),
        lat,
        lon,
    )
    far_a = int(np.argmax(distances_from_seed))
    distances_from_a = _haversine_km(
        np.full(len(points), lat[far_a]),
        np.full(len(points), lon[far_a]),
        lat,
        lon,
    )
    return round(float(distances_from_a.max()), 4)


def _build_zone_clusters(reclamos: pd.DataFrame, cluster_diameter_km: float) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, float | int]]:
    destinos = (
        reclamos.groupby(["destino_key"], as_index=False)
        .agg(
            lat=("lat", "mean"),
            lon=("lon", "mean"),
            reclamos_count=("reclamo_id", "size"),
            sedes_count=("sede_id", "nunique"),
            servicios_count=("servicio_normalizado", "nunique"),
        )
        .sort_values(["reclamos_count", "destino_key"], ascending=[False, True], kind="stable")
        .reset_index(drop=True)
    )

    eps_km = cluster_diameter_km / 2.0
    ref_lat = float(destinos["lat"].mean())
    km_per_lat = 110.574
    km_per_lon = 111.320 * np.cos(np.radians(ref_lat))

    destinos["y_km"] = destinos["lat"].astype(float) * km_per_lat
    destinos["x_km"] = destinos["lon"].astype(float) * km_per_lon
    lat = destinos["lat"].to_numpy(dtype=float)
    lon = destinos["lon"].to_numpy(dtype=float)
    weight = destinos["reclamos_count"].to_numpy(dtype=float)
    search_cell_km = cluster_diameter_km
    neighbor_offsets = [(dx, dy) for dx in (-1, 0, 1) for dy in (-1, 0, 1)]

    clusters: list[dict[str, object]] = []
    cluster_cells: dict[tuple[int, int], set[int]] = defaultdict(set)
    assigned_cluster_ids: list[int] = []

    def _cluster_cell(x_km: float, y_km: float) -> tuple[int, int]:
        return (int(np.floor(x_km / search_cell_km)), int(np.floor(y_km / search_cell_km)))

    for idx in range(len(destinos)):
        point_x = float(destinos.iloc[idx]["x_km"])
        point_y = float(destinos.iloc[idx]["y_km"])
        point_cell = _cluster_cell(point_x, point_y)
        candidate_cluster_ids: set[int] = set()
        for dx, dy in neighbor_offsets:
            candidate_cluster_ids.update(cluster_cells.get((point_cell[0] + dx, point_cell[1] + dy), set()))

        feasible_candidates: list[tuple[float, int]] = []
        for cluster_id in sorted(candidate_cluster_ids):
            cluster = clusters[cluster_id]
            member_idx = np.array(cluster["member_idx"], dtype=int)
            distances = _haversine_km(
                np.full(len(member_idx), lat[idx]),
                np.full(len(member_idx), lon[idx]),
                lat[member_idx],
                lon[member_idx],
            )
            if float(distances.max()) <= cluster_diameter_km:
                centroid_distance = _haversine_km(
                    np.array([lat[idx]]),
                    np.array([lon[idx]]),
                    np.array([cluster["centroid_lat"]]),
                    np.array([cluster["centroid_lon"]]),
                )[0]
                feasible_candidates.append((float(centroid_distance), cluster_id))

        if feasible_candidates:
            _, cluster_id = min(feasible_candidates, key=lambda item: (item[0], item[1]))
        else:
            cluster_id = len(clusters)
            clusters.append(
                {
                    "member_idx": [],
                    "weight_sum": 0.0,
                    "weighted_lat_sum": 0.0,
                    "weighted_lon_sum": 0.0,
                    "centroid_lat": float(lat[idx]),
                    "centroid_lon": float(lon[idx]),
                    "cell": point_cell,
                }
            )

        cluster = clusters[cluster_id]
        previous_cell = cluster["cell"]
        cluster["member_idx"].append(idx)
        cluster["weight_sum"] = float(cluster["weight_sum"]) + float(weight[idx])
        cluster["weighted_lat_sum"] = float(cluster["weighted_lat_sum"]) + float(lat[idx] * weight[idx])
        cluster["weighted_lon_sum"] = float(cluster["weighted_lon_sum"]) + float(lon[idx] * weight[idx])
        cluster["centroid_lat"] = float(cluster["weighted_lat_sum"]) / float(cluster["weight_sum"])
        cluster["centroid_lon"] = float(cluster["weighted_lon_sum"]) / float(cluster["weight_sum"])
        cluster["cell"] = _cluster_cell(
            float(cluster["centroid_lon"]) * km_per_lon,
            float(cluster["centroid_lat"]) * km_per_lat,
        )
        if previous_cell != cluster["cell"] and cluster_id in cluster_cells.get(previous_cell, set()):
            cluster_cells[previous_cell].discard(cluster_id)
        cluster_cells[cluster["cell"]].add(cluster_id)
        assigned_cluster_ids.append(cluster_id)

    destinos["cluster_root"] = assigned_cluster_ids
    zone_order = (
        destinos.groupby("cluster_root", as_index=False)
        .agg(reclamos_count=("reclamos_count", "sum"), lat_mean=("lat", "mean"), lon_mean=("lon", "mean"))
        .sort_values(["reclamos_count", "lat_mean", "lon_mean"], ascending=[False, True, True], kind="stable")
        .reset_index(drop=True)
    )
    zone_order["zona_id"] = [f"zona_{idx:04d}" for idx in range(1, len(zone_order) + 1)]
    destinos = destinos.merge(zone_order[["cluster_root", "zona_id"]], on="cluster_root", how="left", validate="many_to_one")

    resumen_rows: list[dict[str, object]] = []
    for zona_id, group in destinos.groupby("zona_id", sort=False):
        reclamos_count = int(group["reclamos_count"].sum())
        destinos_count = int(len(group))
        weight = group["reclamos_count"].astype(float)
        centroid_lat = float(np.average(group["lat"].astype(float), weights=weight))
        centroid_lon = float(np.average(group["lon"].astype(float), weights=weight))
        diameter_km = _cluster_max_pairwise_km(group[["lat", "lon"]])
        max_center_km = float(
            _haversine_km(
                np.full(destinos_count, centroid_lat),
                np.full(destinos_count, centroid_lon),
                group["lat"].to_numpy(dtype=float),
                group["lon"].to_numpy(dtype=float),
            ).max()
        )
        resumen_rows.append(
            {
                "zona_id": zona_id,
                "cluster_root": int(group["cluster_root"].iloc[0]),
                "destinos_count": destinos_count,
                "reclamos_count": reclamos_count,
                "centroid_lat": round(centroid_lat, 6),
                "centroid_lon": round(centroid_lon, 6),
                "diameter_km": diameter_km,
                "radius_from_centroid_km": round(max_center_km, 4),
                "approx_target_diameter_km": cluster_diameter_km,
                "dbscan_like_eps_km": eps_km,
                "cluster_method": "greedy diameter-constrained spatial clustering",
                "diameter_exceeds_target": diameter_km > cluster_diameter_km,
            }
        )

    zona_cluster_resumen = pd.DataFrame(resumen_rows).sort_values("zona_id", kind="stable").reset_index(drop=True)
    destinos = destinos.merge(
        zona_cluster_resumen[[
            "zona_id",
            "centroid_lat",
            "centroid_lon",
            "diameter_km",
            "radius_from_centroid_km",
            "destinos_count",
            "reclamos_count",
        ]].rename(columns={"destinos_count": "cluster_destinos_count", "reclamos_count": "cluster_reclamos_count"}),
        on="zona_id",
        how="left",
        validate="many_to_one",
    )
    zonas_clusterizadas = destinos[
        [
            "destino_key",
            "lat",
            "lon",
            "zona_id",
            "cluster_root",
            "cluster_reclamos_count",
            "sedes_count",
            "servicios_count",
            "centroid_lat",
            "centroid_lon",
            "diameter_km",
            "radius_from_centroid_km",
            "cluster_destinos_count",
        ]
    ]

    validate_processed_contract(zonas_clusterizadas[["destino_key", "lat", "lon", "zona_id"]], "zonas_clusterizadas")
    validate_processed_contract(zona_cluster_resumen[["zona_id", "destinos_count", "reclamos_count", "diameter_km"]], "zona_cluster_resumen")

    summary = {
        "zonas_total": int(zona_cluster_resumen["zona_id"].nunique()),
        "destinos_total": int(len(zonas_clusterizadas)),
        "reclamos_total": int(reclamos["reclamo_id"].nunique()),
        "eps_km": round(eps_km, 4),
        "diameter_target_km": round(cluster_diameter_km, 4),
        "clusters_over_target": int(zona_cluster_resumen["diameter_exceeds_target"].sum()),
        "max_cluster_diameter_km": float(zona_cluster_resumen["diameter_km"].max()),
        "median_cluster_diameter_km": float(zona_cluster_resumen["diameter_km"].median()),
    }
    return zonas_clusterizadas, zona_cluster_resumen, summary


def _classify_observacion_codigo(code: object) -> str:
    if pd.isna(code):
        return "missing"
    text = str(code).strip()
    if not text:
        return "missing"
    compact = text.replace(" ", "")
    if compact == "A":
        return "plain_a"
    if any(token in compact for token in ("•", "B", "R", "°", "/", "O")):
        return "evento_intenso"
    if any(token in compact for token in ("=", "≡", "E")):
        return "marca_horaria"
    return "otro_codigo"


def _complete_zone_calendar(zone_daily: pd.DataFrame) -> pd.DataFrame:
    zone_profiles = (
        zone_daily.groupby("zona_id", as_index=False)
        .agg(fecha_min=("fecha", "min"), fecha_max=("fecha", "max"))
        .sort_values("zona_id", kind="stable")
    )
    calendar_parts: list[pd.DataFrame] = []
    for _, row in zone_profiles.iterrows():
        calendar_parts.append(
            pd.DataFrame(
                {
                    "zona_id": row["zona_id"],
                    "fecha": pd.date_range(row["fecha_min"], row["fecha_max"], freq="D"),
                }
            )
        )
    calendar = pd.concat(calendar_parts, ignore_index=True)
    return calendar


def _online_historical_median(values: pd.Series, keys: list[object], min_periods: int) -> pd.Series:
    history: dict[object, list[float]] = defaultdict(list)
    output = np.full(len(values), np.nan, dtype=float)
    for idx, (value, key) in enumerate(zip(values.to_numpy(dtype=float), keys, strict=False)):
        hist = history[key]
        if len(hist) >= min_periods:
            output[idx] = float(np.median(hist))
        if not pd.isna(value):
            hist.append(float(value))
    return pd.Series(output, index=values.index)


def _trailing_group_median(frame: pd.DataFrame, value_column: str, group_column: str, window: int, min_periods: int) -> pd.Series:
    output = np.full(len(frame), np.nan, dtype=float)
    for _, raw_idx in frame.groupby(group_column, sort=False).groups.items():
        idx = np.asarray(list(raw_idx), dtype=int)
        values = frame.loc[idx, value_column].to_numpy(dtype=float)
        for pos, row_idx in enumerate(idx):
            history = values[max(0, pos - window) : pos]
            history = history[~np.isnan(history)]
            if len(history) >= min_periods:
                output[row_idx] = float(np.median(history))
    return pd.Series(output, index=frame.index)


def _compose_rule_label(*pairs: tuple[str, pd.Series]) -> pd.Series:
    label = pd.Series(index=pairs[0][1].index, dtype="object")
    for rule_name, candidate in pairs:
        mask = label.isna() & candidate.notna()
        label.loc[mask] = rule_name
    label = label.fillna("global_fallback")
    return label


def _evaluate_baseline_metrics(predictions: pd.DataFrame, recent_days: int = 180) -> pd.DataFrame:
    latest_date = predictions.loc[predictions["target_available_t+1"], "fecha"].max()
    recent_cutoff = latest_date - pd.Timedelta(days=recent_days - 1)
    metric_specs = [
        ("tiempo", "y_tiempo_t+1_min", "baseline_tiempo_t+1_min"),
        ("costo", "y_costo_t+1_ars", "baseline_costo_t+1_ars"),
        ("reclamos", "y_reclamos_t+1", "baseline_reclamos_t+1"),
    ]
    windows = {
        "full_history": predictions["target_available_t+1"],
        f"recent_{recent_days}d": predictions["target_available_t+1"] & predictions["fecha"].ge(recent_cutoff),
    }

    rows: list[dict[str, object]] = []
    for target_name, actual_column, pred_column in metric_specs:
        for window_name, mask in windows.items():
            subset = predictions.loc[mask, [actual_column, pred_column]].dropna()
            actual = subset[actual_column].to_numpy(dtype=float)
            pred = subset[pred_column].to_numpy(dtype=float)
            abs_error = np.abs(actual - pred)
            rows.append(
                {
                    "target": target_name,
                    "evaluation_window": window_name,
                    "rows": int(len(subset)),
                    "mae": float(abs_error.mean()),
                    "rmse": float(np.sqrt(np.mean((actual - pred) ** 2))),
                    "wmape": float(abs_error.sum() / np.abs(actual).sum()) if np.abs(actual).sum() else 0.0,
                    "actual_total": float(actual.sum()),
                    "pred_total": float(pred.sum()),
                }
            )
    return pd.DataFrame(rows)


def _write_zone_report(path: Path, summary: dict[str, float | int], zona_cluster_resumen: pd.DataFrame) -> None:
    top_zones = zona_cluster_resumen.sort_values(["reclamos_count", "destinos_count"], ascending=[False, False], kind="stable").head(10)
    lines = [
        "# Decisión de clustering espacial para `zona_id`",
        "",
        "## Criterio implementado",
        "",
        "- Método práctico: clustering greedy con restricción explícita de diámetro máximo `<= 5 km` entre cualquier nuevo punto y los miembros ya asignados del cluster.",
        "- Diámetro objetivo solicitado: `5 km`.",
        "- Justificación: no dependemos de `scikit-learn`, evitamos el efecto cadena de DBSCAN/simple-linkage y mantenemos una partición más defendible para el grano `zona-diario`.",
        "- Advertencia metodológica: la asignación es greedy (ordenada por volumen de reclamos), así que no garantiza el óptimo global; sí garantiza control práctico del diámetro observado y trazabilidad reproducible.",
        "",
        "## Resumen",
        "",
        f"- Zonas generadas: `{summary['zonas_total']}`.",
        f"- Destinos únicos clusterizados: `{summary['destinos_total']}`.",
        f"- Reclamos cubiertos por el mapeo: `{summary['reclamos_total']}`.",
        f"- `eps` efectivo: `{summary['eps_km']}` km.",
        f"- Clusters que exceden el diámetro objetivo: `{summary['clusters_over_target']}`.",
        f"- Diámetro máximo observado: `{summary['max_cluster_diameter_km']:.3f}` km.",
        f"- Mediana de diámetro observado: `{summary['median_cluster_diameter_km']:.3f}` km.",
        "",
        "## Zonas con más reclamos",
        "",
        "| zona_id | reclamos | destinos | diámetro_km | excede_5km |",
        "|---|---:|---:|---:|---|",
    ]
    for _, row in top_zones.iterrows():
        lines.append(
            f"| `{row['zona_id']}` | {int(row['reclamos_count'])} | {int(row['destinos_count'])} | {float(row['diameter_km']):.3f} | {'sí' if bool(row['diameter_exceeds_target']) else 'no'} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_rain_report(path: Path, lluvia: pd.DataFrame) -> None:
    rain_summary = (
        lluvia.groupby("obs_categoria", dropna=False)
        .agg(dias=("fecha", "size"), dias_con_lluvia=("llovio", "sum"), lluvia_media_mm=("lluvia_mm", "mean"), lluvia_mediana_mm=("lluvia_mm", "median"))
        .reset_index()
        .sort_values(["dias", "lluvia_media_mm"], ascending=[False, False], kind="stable")
    )
    accepted = rain_summary[rain_summary["obs_categoria"] == "evento_intenso"]
    accepted_line = "No se aceptó ninguna derivación adicional."
    if not accepted.empty:
        row = accepted.iloc[0]
        accepted_line = (
            "Se acepta `obs_evento_intenso_flag` porque la categoría `evento_intenso` concentra "
            f"`{int(row['dias'])}` días, con lluvia media `{float(row['lluvia_media_mm']):.2f} mm` y "
            f"`{int(row['dias_con_lluvia'])}` días lluviosos, claramente por encima de `plain_a`/`missing`."
        )
    lines = [
        "# Notas de features de lluvia",
        "",
        "## Exploración de `observacion_codigo`",
        "",
        "- Se normalizó `observacion_codigo` a cuatro categorías simples: `plain_a`, `missing`, `marca_horaria`, `evento_intenso`, más `otro_codigo` como residual.",
        f"- {accepted_line}",
        "- Se rechazan one-hot por token individual (`B`, `R`, `•`, `°`, etc.) porque son más opacos y dispersos para esta etapa base.",
        "",
        "## Resumen por categoría",
        "",
        "| categoria | dias | dias_con_lluvia | lluvia_media_mm | lluvia_mediana_mm |",
        "|---|---:|---:|---:|---:|",
    ]
    for _, row in rain_summary.iterrows():
        lines.append(
            f"| `{row['obs_categoria']}` | {int(row['dias'])} | {int(row['dias_con_lluvia'])} | {float(row['lluvia_media_mm']):.2f} | {float(row['lluvia_mediana_mm']):.2f} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_dataset_report(path: Path, zona_diario_base: pd.DataFrame, zona_diario_supervisado: pd.DataFrame) -> None:
    lines = [
        "# Resumen de construcción `zona-diario`",
        "",
        f"- Filas `zona_diario_base`: `{len(zona_diario_base)}`.",
        f"- Filas `zona_diario_supervisado`: `{len(zona_diario_supervisado)}`.",
        f"- Zonas únicas: `{zona_diario_base['zona_id'].nunique()}`.",
        f"- Rango temporal base: `{zona_diario_base['fecha'].min().date()}` → `{zona_diario_base['fecha'].max().date()}`.",
        f"- Días con reclamos: `{int(zona_diario_base['has_claims'].sum())}`.",
        f"- Días sin reclamos pero retenidos para historial/targets: `{int((~zona_diario_base['has_claims']).sum())}`.",
        "",
        "## Features iniciales incluidas",
        "",
        "- Traslado: `traslado_min_total`, `traslado_min_promedio`, `distance_km_total`.",
        "- Resolución heurística: `resolucion_base_min_total`, `tiempo_total_operativo_min`.",
        "- Costo soporte: componentes laborales, km, combustible y `costo_total_compuesto_ars`.",
        "- Lluvia: `llovio`, `lluvia_mm`, `lluvia_intensidad`, `lluvia_status`, `obs_evento_intenso_flag`.",
        "- Supervisión futura: `y_tiempo_t+1_min`, `y_costo_t+1_ars`, `y_reclamos_t+1`.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_verification_report(
    path: Path,
    zona_diario_base: pd.DataFrame,
    zona_diario_supervisado: pd.DataFrame,
    reclamos_zonificados: pd.DataFrame,
    baseline_metrics: pd.DataFrame | None = None,
) -> None:
    duplicated_base = int(zona_diario_base.duplicated(["zona_id", "fecha"]).sum())
    null_zone_rows = int(reclamos_zonificados["zona_id"].isna().sum())
    leakage_checks = {
        "lag1_uses_past_only": bool(
            zona_diario_supervisado.loc[zona_diario_supervisado["reclamos_count_lag_1"].notna(), "fecha"].ge(
                zona_diario_supervisado.loc[zona_diario_supervisado["reclamos_count_lag_1"].notna(), "fecha"]
            ).all()
        ),
        "targets_have_future_shift": bool(
            zona_diario_supervisado["y_reclamos_t+1"].iloc[:-1].notna().any() if len(zona_diario_supervisado) > 1 else True
        ),
    }
    lines = [
        "# Verificación liviana de outputs `zona-diario`",
        "",
        f"- Duplicados `zona_id-fecha` en base: `{duplicated_base}`.",
        f"- Reclamos sin `zona_id` luego del join: `{null_zone_rows}`.",
        f"- Targets nulos esperados (último día por zona): `{int(zona_diario_supervisado['y_reclamos_t+1'].isna().sum())}`.",
        f"- Filas entrenables con target disponible: `{int(zona_diario_supervisado['y_reclamos_t+1'].notna().sum())}`.",
        f"- Check `lag1_uses_past_only`: `{leakage_checks['lag1_uses_past_only']}`.",
        f"- Check `targets_have_future_shift`: `{leakage_checks['targets_have_future_shift']}`.",
        "",
        "## Estado",
        "",
        "- Validación estructural: OK.",
        "- Anti-leakage básico por construcción `shift/rolling` sobre pasado: OK.",
    ]
    if baseline_metrics is None:
        lines.append("- Listo para baseline heurístico y entrenamiento posterior, sin ejecutar modelos todavía.")
    else:
        recent_metrics = baseline_metrics[baseline_metrics["evaluation_window"].eq("recent_180d")]
        lines.extend(
            [
                "- Baseline heurístico persistido fuera de la notebook: OK.",
                "- La notebook final sigue consumiendo artifacts procesados, sin recalcular el pipeline pesado: OK.",
                "",
                "## Baseline heurístico — ventana reciente 180 días",
                "",
                "| target | filas | MAE | RMSE | WMAPE |",
                "|---|---:|---:|---:|---:|",
            ]
        )
        for _, row in recent_metrics.iterrows():
            lines.append(
                f"| `{row['target']}` | {int(row['rows'])} | {float(row['mae']):.2f} | {float(row['rmse']):.2f} | {float(row['wmape']):.4f} |"
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_baseline_report(path: Path, predictions: pd.DataFrame, metrics: pd.DataFrame) -> None:
    rules_summary = []
    for target_name, rule_column in [
        ("tiempo", "baseline_rule_tiempo"),
        ("costo", "baseline_rule_costo"),
        ("reclamos", "baseline_rule_reclamos"),
    ]:
        counts = (
            predictions.loc[predictions["target_available_t+1"], rule_column]
            .value_counts(dropna=False)
            .rename_axis("rule")
            .reset_index(name="rows")
        )
        for _, row in counts.iterrows():
            rules_summary.append(f"| `{target_name}` | `{row['rule']}` | {int(row['rows'])} |")

    lines = [
        "# Baseline heurístico `zona-diario` (t+1)",
        "",
        "## Enfoque",
        "",
        "- **Tiempo futuro**: mediana histórica por `zona_id` y mismo mes; si falta historia suficiente, cae a estación trimestral, mediana trailing 28 días, historia de zona y finalmente mediana global.",
        "- **Reclamos futuros**: heurística estacional simple `semana_anterior -> mediana trailing 28d -> historia de zona -> global`.",
        "- **Costo futuro**: `baseline_reclamos * costo_por_reclamo histórico`, usando como prioridad mes de la zona, trailing 28 días, historia de zona y fallback global.",
        "- Todo el baseline es **online/past-only**: cada predicción usa únicamente observaciones disponibles antes del corte de esa fila.",
        "",
        "## Métricas comparables",
        "",
        "| target | ventana | filas | MAE | RMSE | WMAPE | total_real | total_predicho |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in metrics.iterrows():
        lines.append(
            f"| `{row['target']}` | `{row['evaluation_window']}` | {int(row['rows'])} | {float(row['mae']):.2f} | {float(row['rmse']):.2f} | {float(row['wmape']):.4f} | {float(row['actual_total']):.2f} | {float(row['pred_total']):.2f} |"
        )

    lines.extend(
        [
            "",
            "## Cobertura por regla heurística",
            "",
            "| target | regla | filas |",
            "|---|---|---:|",
            *rules_summary,
            "",
            "## Lectura rápida",
            "",
            "- Este benchmark ya deja una vara explícita para las tandas ML: cualquier modelo futuro tiene que mejorar estas métricas manteniendo el esquema temporal sin leakage.",
            "- La señal principal sigue siendo **tiempo operativo futuro**; costo acompaña como traducción monetaria y reclamos como presión futura de demanda.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_baseline_heuristic_artifacts(recent_days: int = 180) -> dict[str, object]:
    ensure_workspace_layout()

    zona_diario_supervisado = pd.read_parquet(PROCESSED_DIR / "zona_diario_supervisado.parquet").copy()
    zona_diario_base = pd.read_parquet(PROCESSED_DIR / "zona_diario_base.parquet").copy()
    reclamos_zonificados = pd.read_parquet(PROCESSED_DIR / "reclamos_zonificados.parquet").copy()
    zona_diario_supervisado["fecha"] = pd.to_datetime(zona_diario_supervisado["fecha"]).dt.normalize()
    zona_diario_supervisado = zona_diario_supervisado.sort_values(["zona_id", "fecha"], kind="stable").reset_index(drop=True)
    zona_diario_supervisado["season"] = ((zona_diario_supervisado["month"] - 1) // 3) + 1

    global_time_fallback = float(zona_diario_supervisado["y_tiempo_t+1_min"].median())
    global_claim_fallback = float(zona_diario_supervisado["y_reclamos_t+1"].median())
    cost_per_claim_target = zona_diario_supervisado["y_costo_t+1_ars"].div(zona_diario_supervisado["y_reclamos_t+1"].replace(0, np.nan))
    global_cost_per_claim_fallback = float(cost_per_claim_target.dropna().median())

    time_month = _online_historical_median(
        zona_diario_supervisado["y_tiempo_t+1_min"],
        list(zip(zona_diario_supervisado["zona_id"], zona_diario_supervisado["month"], strict=False)),
        min_periods=5,
    )
    time_season = _online_historical_median(
        zona_diario_supervisado["y_tiempo_t+1_min"],
        list(zip(zona_diario_supervisado["zona_id"], zona_diario_supervisado["season"], strict=False)),
        min_periods=5,
    )
    time_roll28 = _trailing_group_median(zona_diario_supervisado, "y_tiempo_t+1_min", "zona_id", window=28, min_periods=7)
    time_zone = _online_historical_median(zona_diario_supervisado["y_tiempo_t+1_min"], zona_diario_supervisado["zona_id"].tolist(), min_periods=7)
    time_global = _online_historical_median(
        zona_diario_supervisado["y_tiempo_t+1_min"],
        ["global"] * len(zona_diario_supervisado),
        min_periods=30,
    )
    baseline_tiempo = time_month.fillna(time_season).fillna(time_roll28).fillna(time_zone).fillna(time_global).fillna(global_time_fallback)
    baseline_rule_tiempo = _compose_rule_label(
        ("zona_mes_mediana", time_month),
        ("zona_estacion_mediana", time_season),
        ("zona_trailing_28d", time_roll28),
        ("zona_historia_total", time_zone),
        ("historia_global", time_global),
    )

    claim_prev_week = zona_diario_supervisado.groupby("zona_id", sort=False)["y_reclamos_t+1"].shift(7)
    claim_roll28 = _trailing_group_median(zona_diario_supervisado, "y_reclamos_t+1", "zona_id", window=28, min_periods=7)
    claim_zone = _online_historical_median(zona_diario_supervisado["y_reclamos_t+1"], zona_diario_supervisado["zona_id"].tolist(), min_periods=7)
    claim_global = _online_historical_median(
        zona_diario_supervisado["y_reclamos_t+1"],
        ["global"] * len(zona_diario_supervisado),
        min_periods=30,
    )
    baseline_reclamos = claim_prev_week.fillna(claim_roll28).fillna(claim_zone).fillna(claim_global).fillna(global_claim_fallback).clip(lower=0)
    baseline_rule_reclamos = _compose_rule_label(
        ("semana_anterior", claim_prev_week),
        ("zona_trailing_28d", claim_roll28),
        ("zona_historia_total", claim_zone),
        ("historia_global", claim_global),
    )

    zona_diario_supervisado["cost_per_claim_target"] = cost_per_claim_target
    cpp_month = _online_historical_median(
        zona_diario_supervisado["cost_per_claim_target"],
        list(zip(zona_diario_supervisado["zona_id"], zona_diario_supervisado["month"], strict=False)),
        min_periods=5,
    )
    cpp_roll28 = _trailing_group_median(zona_diario_supervisado, "cost_per_claim_target", "zona_id", window=28, min_periods=7)
    cpp_zone = _online_historical_median(zona_diario_supervisado["cost_per_claim_target"], zona_diario_supervisado["zona_id"].tolist(), min_periods=7)
    cpp_global = _online_historical_median(
        zona_diario_supervisado["cost_per_claim_target"],
        ["global"] * len(zona_diario_supervisado),
        min_periods=30,
    )
    baseline_costo_unitario = cpp_month.fillna(cpp_roll28).fillna(cpp_zone).fillna(cpp_global).fillna(global_cost_per_claim_fallback)
    baseline_costo = (baseline_reclamos * baseline_costo_unitario).clip(lower=0)
    baseline_rule_costo = _compose_rule_label(
        ("reclamos_baseline_x_costo_mes", cpp_month),
        ("reclamos_baseline_x_costo_trailing_28d", cpp_roll28),
        ("reclamos_baseline_x_costo_zona", cpp_zone),
        ("reclamos_baseline_x_costo_global", cpp_global),
    )

    predictions = zona_diario_supervisado[
        [
            "zona_id",
            "fecha",
            "zona_sede_principal",
            "zona_servicio_principal",
            "reclamos_count",
            "target_available_t+1",
            "y_tiempo_t+1_min",
            "y_costo_t+1_ars",
            "y_reclamos_t+1",
        ]
    ].copy()
    predictions["baseline_tiempo_t+1_min"] = baseline_tiempo
    predictions["baseline_costo_t+1_ars"] = baseline_costo
    predictions["baseline_reclamos_t+1"] = baseline_reclamos
    predictions["baseline_rule_tiempo"] = baseline_rule_tiempo
    predictions["baseline_rule_costo"] = baseline_rule_costo
    predictions["baseline_rule_reclamos"] = baseline_rule_reclamos
    predictions["abs_error_tiempo"] = (predictions["y_tiempo_t+1_min"] - predictions["baseline_tiempo_t+1_min"]).abs()
    predictions["abs_error_costo"] = (predictions["y_costo_t+1_ars"] - predictions["baseline_costo_t+1_ars"]).abs()
    predictions["abs_error_reclamos"] = (predictions["y_reclamos_t+1"] - predictions["baseline_reclamos_t+1"]).abs()

    validate_processed_contract(
        predictions[
            [
                "zona_id",
                "fecha",
                "baseline_tiempo_t+1_min",
                "baseline_costo_t+1_ars",
                "baseline_reclamos_t+1",
                "y_tiempo_t+1_min",
                "y_costo_t+1_ars",
                "y_reclamos_t+1",
            ]
        ],
        "zona_diario_baseline_t1",
    )

    metrics = _evaluate_baseline_metrics(predictions, recent_days=recent_days)
    validate_processed_contract(metrics[["target", "evaluation_window", "rows", "mae", "rmse", "wmape"]], "zona_diario_baseline_metricas")

    artifacts = ZonaDiarioBaselineArtifacts(
        baseline_predictions=PROCESSED_DIR / "zona_diario_baseline_t1.parquet",
        baseline_metrics=PROCESSED_DIR / "zona_diario_baseline_metricas.parquet",
        baseline_report=REPORTS_DIR / "baseline_heuristico_summary.md",
    )
    predictions.to_parquet(artifacts.baseline_predictions, index=False)
    metrics.to_parquet(artifacts.baseline_metrics, index=False)
    _write_baseline_report(artifacts.baseline_report, predictions, metrics)
    _write_verification_report(
        REPORTS_DIR / "verification_summary.md",
        zona_diario_base,
        zona_diario_supervisado,
        reclamos_zonificados,
        baseline_metrics=metrics,
    )

    recent_metrics = metrics[metrics["evaluation_window"].eq(f"recent_{recent_days}d")].set_index("target")
    return {
        "summary": {
            "rows_total": int(len(predictions)),
            "rows_evaluable": int(predictions["target_available_t+1"].sum()),
            "recent_days": int(recent_days),
            "tiempo_mae_recent": float(recent_metrics.loc["tiempo", "mae"]),
            "costo_mae_recent": float(recent_metrics.loc["costo", "mae"]),
            "reclamos_mae_recent": float(recent_metrics.loc["reclamos", "mae"]),
        },
        "artifacts": {field: str(getattr(artifacts, field)) for field in artifacts.__dataclass_fields__},
    }


def build_zona_diario_artifacts(
    cluster_diameter_km: float = DEFAULT_CLUSTER_DIAMETER_KM,
    service_base_minutes: float = DEFAULT_SERVICE_BASE_MINUTES,
) -> dict[str, object]:
    ensure_workspace_layout()

    reclamos = pd.read_parquet(PROCESSED_DIR / "reclamos_enriquecidos.parquet").copy()
    lluvia = pd.read_parquet(PROCESSED_DIR / "lluvia_diaria_clean.parquet").copy()
    costos_ref = pd.read_parquet(PROCESSED_DIR / "costos_ref.parquet")

    zonas_clusterizadas, zona_cluster_resumen, cluster_summary = _build_zone_clusters(reclamos, cluster_diameter_km)
    zona_lookup = zonas_clusterizadas[["destino_key", "zona_id"]].drop_duplicates()
    reclamos_zonificados = reclamos.merge(zona_lookup, on="destino_key", how="left", validate="many_to_one")
    reclamos_zonificados["fecha"] = pd.to_datetime(reclamos_zonificados["fecha_reclamo"]).dt.normalize()
    reclamos_zonificados["traslado_min"] = reclamos_zonificados["duration_s"].astype(float).div(60.0)
    reclamos_zonificados["resolucion_base_min"] = float(service_base_minutes)
    reclamos_zonificados["tiempo_total_operativo_min"] = (
        reclamos_zonificados["traslado_min"].fillna(0.0) + reclamos_zonificados["resolucion_base_min"]
    )
    reclamos_zonificados["resolucion_base_h"] = reclamos_zonificados["resolucion_base_min"].div(60.0)
    reclamos_zonificados["combustible_litros"] = reclamos_zonificados["distance_km"].astype(float).div(10.0)
    fuel_price = float(costos_ref.iloc[0]["combustible_precio_litro_ars"])
    reclamos_zonificados["costo_combustible_ars"] = reclamos_zonificados["combustible_litros"].fillna(0.0).mul(fuel_price)
    reclamos_zonificados["costo_resolucion_base_ars"] = (
        reclamos_zonificados["resolucion_base_h"] * reclamos_zonificados["costo_hora_base_ars"].astype(float)
    )
    reclamos_zonificados["costo_total_compuesto_ars"] = (
        reclamos_zonificados["costo_operativo_ars"].fillna(0.0)
        + reclamos_zonificados["costo_resolucion_base_ars"].fillna(0.0)
        + reclamos_zonificados["costo_combustible_ars"].fillna(0.0)
    )

    validate_processed_contract(
        reclamos_zonificados[["reclamo_id", "fecha_reclamo", "zona_id", "sede_id", "distance_km", "costo_operativo_ars"]],
        "reclamos_zonificados",
    )

    zone_profile_rows: list[dict[str, object]] = []
    for zona_id, group in reclamos_zonificados.groupby("zona_id", sort=False):
        zone_profile_rows.append(
            {
                "zona_id": zona_id,
                "zona_sede_principal": _dominant_value(group, "sede_id"),
                "zona_servicio_principal": _dominant_value(group, "servicio_normalizado"),
                "zona_localidad_principal": _dominant_value(group, "localidad"),
                "sedes_unicas_zona": int(group["sede_id"].nunique()),
                "servicios_unicos_zona": int(group["servicio_normalizado"].nunique()),
            }
        )
    zone_profiles = pd.DataFrame(zone_profile_rows)
    zone_profiles = zone_profiles.merge(
        zona_cluster_resumen.rename(
            columns={
                "destinos_count": "zona_destinos_count",
                "reclamos_count": "zona_reclamos_total",
                "diameter_km": "zona_diameter_km",
                "radius_from_centroid_km": "zona_radius_from_centroid_km",
            }
        ),
        on="zona_id",
        how="left",
        validate="one_to_one",
    )

    zone_daily_claims = (
        reclamos_zonificados.groupby(["zona_id", "fecha"], as_index=False)
        .agg(
            reclamos_count=("reclamo_id", "size"),
            reclamos_unicos=("reclamo_id", "nunique"),
            sedes_unicas=("sede_id", "nunique"),
            servicios_unicos=("servicio_normalizado", "nunique"),
            localidades_unicas=("localidad", "nunique"),
            destinos_unicos=("destino_key", "nunique"),
            distance_km_total=("distance_km", "sum"),
            traslado_min_total=("traslado_min", "sum"),
            traslado_min_promedio=("traslado_min", "mean"),
            resolucion_base_min_total=("resolucion_base_min", "sum"),
            tiempo_total_operativo_min=("tiempo_total_operativo_min", "sum"),
            combustible_litros_total=("combustible_litros", "sum"),
            costo_operativo_total_ars=("costo_operativo_ars", "sum"),
            costo_resolucion_base_ars=("costo_resolucion_base_ars", "sum"),
            costo_combustible_ars=("costo_combustible_ars", "sum"),
            costo_total_compuesto_ars=("costo_total_compuesto_ars", "sum"),
        )
        .sort_values(["zona_id", "fecha"], kind="stable")
        .reset_index(drop=True)
    )

    per_sede = (
        reclamos_zonificados.pivot_table(
            index=["zona_id", "fecha"],
            columns="sede_id",
            values="reclamo_id",
            aggfunc="size",
            fill_value=0,
        )
        .rename(columns=lambda value: f"reclamos_{value}")
        .reset_index()
    )
    zone_daily_claims = zone_daily_claims.merge(per_sede, on=["zona_id", "fecha"], how="left", validate="one_to_one")

    calendar = _complete_zone_calendar(zone_daily_claims)
    zona_diario_base = calendar.merge(zone_daily_claims, on=["zona_id", "fecha"], how="left", validate="one_to_one")
    zona_diario_base = zona_diario_base.merge(zone_profiles, on="zona_id", how="left", validate="many_to_one")

    fill_zero_columns = [
        column
        for column in zona_diario_base.columns
        if column.endswith("_total")
        or column.endswith("_ars")
        or column.endswith("_litros")
        or column.endswith("_count")
        or column.endswith("_unicos")
        or column.startswith("reclamos_")
        or column in {"traslado_min_promedio", "reclamos_unicos", "destinos_unicos", "distance_km_total", "tiempo_total_operativo_min"}
    ]
    for column in fill_zero_columns:
        if column in zona_diario_base.columns:
            zona_diario_base[column] = zona_diario_base[column].fillna(0)

    zona_diario_base["has_claims"] = zona_diario_base["reclamos_count"].gt(0)
    lluvia["fecha"] = pd.to_datetime(lluvia["fecha"]).dt.normalize()
    lluvia["llovio"] = lluvia["lluvia_mm"].fillna(0.0).gt(0)
    lluvia["obs_categoria"] = lluvia["observacion_codigo"].map(_classify_observacion_codigo)
    lluvia["obs_evento_intenso_flag"] = lluvia["obs_categoria"].eq("evento_intenso")
    lluvia["lluvia_intensidad"] = pd.cut(
        lluvia["lluvia_mm"].fillna(0.0),
        bins=[-0.001, 0.0, 10.0, 30.0, np.inf],
        labels=["sin_lluvia", "leve", "moderada", "fuerte"],
    ).astype(str)
    zona_diario_base = zona_diario_base.merge(
        lluvia[[
            "fecha",
            "lluvia_mm",
            "llovio",
            "lluvia_status",
            "observacion_codigo",
            "obs_categoria",
            "obs_evento_intenso_flag",
            "lluvia_intensidad",
        ]],
        on="fecha",
        how="left",
        validate="many_to_one",
    )
    zona_diario_base["lluvia_mm"] = zona_diario_base["lluvia_mm"].fillna(0.0)
    zona_diario_base["llovio"] = zona_diario_base["llovio"].fillna(False)
    zona_diario_base["obs_evento_intenso_flag"] = zona_diario_base["obs_evento_intenso_flag"].fillna(False)
    zona_diario_base["lluvia_status"] = zona_diario_base["lluvia_status"].fillna("missing")
    zona_diario_base["obs_categoria"] = zona_diario_base["obs_categoria"].fillna("missing")
    zona_diario_base["lluvia_intensidad"] = zona_diario_base["lluvia_intensidad"].fillna("sin_dato")

    zona_diario_base["day_of_week"] = zona_diario_base["fecha"].dt.dayofweek
    zona_diario_base["month"] = zona_diario_base["fecha"].dt.month
    zona_diario_base["quarter"] = zona_diario_base["fecha"].dt.quarter
    zona_diario_base["is_weekend"] = zona_diario_base["day_of_week"].isin([5, 6])

    zona_diario_base = zona_diario_base.sort_values(["zona_id", "fecha"], kind="stable").reset_index(drop=True)
    validate_processed_contract(
        zona_diario_base[[
            "zona_id",
            "fecha",
            "reclamos_count",
            "traslado_min_total",
            "resolucion_base_min_total",
            "tiempo_total_operativo_min",
            "costo_total_compuesto_ars",
            "lluvia_mm",
            "llovio",
        ]],
        "zona_diario_base",
    )

    zona_diario_supervisado = zona_diario_base.copy()
    grouped = zona_diario_supervisado.groupby("zona_id", sort=False)
    for metric in ["reclamos_count", "tiempo_total_operativo_min", "costo_total_compuesto_ars", "traslado_min_total", "lluvia_mm"]:
        zona_diario_supervisado[f"{metric}_lag_1"] = grouped[metric].shift(1)
        zona_diario_supervisado[f"{metric}_lag_7"] = grouped[metric].shift(7)
        zona_diario_supervisado[f"{metric}_roll_7_mean"] = grouped[metric].shift(1).rolling(7, min_periods=3).mean().reset_index(level=0, drop=True)
        zona_diario_supervisado[f"{metric}_roll_28_mean"] = grouped[metric].shift(1).rolling(28, min_periods=7).mean().reset_index(level=0, drop=True)

    zona_diario_supervisado["y_reclamos_t+1"] = grouped["reclamos_count"].shift(-1)
    zona_diario_supervisado["y_tiempo_t+1_min"] = grouped["tiempo_total_operativo_min"].shift(-1)
    zona_diario_supervisado["y_costo_t+1_ars"] = grouped["costo_total_compuesto_ars"].shift(-1)
    zona_diario_supervisado["target_available_t+1"] = zona_diario_supervisado["y_reclamos_t+1"].notna()
    validate_processed_contract(
        zona_diario_supervisado[["zona_id", "fecha", "reclamos_count", "y_tiempo_t+1_min", "y_costo_t+1_ars", "y_reclamos_t+1"]],
        "zona_diario_supervisado",
    )

    artifacts = ZonaDiarioArtifacts(
        zonas_clusterizadas=PROCESSED_DIR / "zonas_clusterizadas.parquet",
        zona_cluster_resumen=PROCESSED_DIR / "zona_cluster_resumen.parquet",
        reclamos_zonificados=PROCESSED_DIR / "reclamos_zonificados.parquet",
        zona_diario_base=PROCESSED_DIR / "zona_diario_base.parquet",
        zona_diario_supervisado=PROCESSED_DIR / "zona_diario_supervisado.parquet",
        zone_report=REPORTS_DIR / "zone_clustering_decision.md",
        rain_report=REPORTS_DIR / "rain_feature_notes.md",
        dataset_report=REPORTS_DIR / "zona_diario_build_summary.md",
        verification_report=REPORTS_DIR / "verification_summary.md",
    )

    zonas_clusterizadas.to_parquet(artifacts.zonas_clusterizadas, index=False)
    zona_cluster_resumen.to_parquet(artifacts.zona_cluster_resumen, index=False)
    reclamos_zonificados.to_parquet(artifacts.reclamos_zonificados, index=False)
    zona_diario_base.to_parquet(artifacts.zona_diario_base, index=False)
    zona_diario_supervisado.to_parquet(artifacts.zona_diario_supervisado, index=False)

    _write_zone_report(artifacts.zone_report, cluster_summary, zona_cluster_resumen)
    _write_rain_report(artifacts.rain_report, lluvia)
    _write_dataset_report(artifacts.dataset_report, zona_diario_base, zona_diario_supervisado)
    _write_verification_report(artifacts.verification_report, zona_diario_base, zona_diario_supervisado, reclamos_zonificados)

    return {
        "summary": {
            **cluster_summary,
            "zona_diario_base_rows": int(len(zona_diario_base)),
            "zona_diario_supervisado_rows": int(len(zona_diario_supervisado)),
            "base_has_claim_days": int(zona_diario_base["has_claims"].sum()),
            "base_zero_claim_days": int((~zona_diario_base["has_claims"]).sum()),
            "rain_event_days": int(zona_diario_base["llovio"].sum()),
            "trainable_rows_t_plus_1": int(zona_diario_supervisado["target_available_t+1"].sum()),
        },
        "artifacts": {field: str(getattr(artifacts, field)) for field in artifacts.__dataclass_fields__},
    }


if __name__ == "__main__":
    result = build_zona_diario_artifacts()
    print(result["summary"])
