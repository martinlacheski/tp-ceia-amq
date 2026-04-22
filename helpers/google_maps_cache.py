from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd

from helpers.costing import build_costing_outputs, build_operational_aggregates, write_reference_outputs
from helpers.operational_refs import build_routing_scope_pairs
from helpers.paths import INTERMEDIATE_DIR, PROCESSED_DIR, REPORTS_DIR, ensure_workspace_layout, get_env_var, require_input


GOOGLE_DIRECTIONS_URL = "https://maps.googleapis.com/maps/api/directions/json"
CACHE_VERSION = "v1"
GOOGLE_SOURCE = "google_maps_directions"
FATAL_API_STATUSES = {"OVER_DAILY_LIMIT", "OVER_QUERY_LIMIT", "REQUEST_DENIED", "INVALID_REQUEST"}


@dataclass(slots=True)
class RoutingArtifacts:
    distancias_cache: Path
    routing_scope: Path
    report: Path


@dataclass(slots=True)
class RoutingAutomationArtifacts:
    batch_history: Path
    report: Path


def _empty_cache_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "sede_id",
            "sede_nombre",
            "sede_lat",
            "sede_lon",
            "destino_key",
            "lat",
            "lon",
            "reclamos_count",
            "servicios_count",
            "servicios",
            "distance_m",
            "duration_s",
            "routing_status",
            "fuente_ruteo",
            "api_status",
            "error_message",
            "requested_at",
            "cache_version",
        ]
    )


def _load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_routing_pairs() -> pd.DataFrame:
    reclamos = pd.read_parquet(PROCESSED_DIR / "reclamos_clean.parquet")
    destinos = build_routing_scope_pairs(reclamos)
    if destinos.duplicated(subset=["sede_id", "destino_key"]).any():
        raise ValueError("El routing scope multi-sede no es único por (sede_id, destino_key)")
    return destinos


def _load_existing_cache(path: Path, routing_pairs: pd.DataFrame | None = None) -> pd.DataFrame:
    if not path.exists():
        return _empty_cache_frame()
    cache = pd.read_parquet(path)
    if cache.empty:
        return _empty_cache_frame()
    cache = cache.sort_values(["requested_at", "sede_id", "destino_key"], kind="stable")
    latest = cache.drop_duplicates(subset=["sede_id", "destino_key"], keep="last")
    if routing_pairs is not None and not latest.empty:
        latest = latest.merge(routing_pairs[["sede_id", "destino_key"]], on=["sede_id", "destino_key"], how="inner")
    return latest.reset_index(drop=True)


def _fetch_route(api_key: str, sede: dict[str, object], destination: pd.Series, timeout_s: int) -> dict[str, object]:
    query = urlencode(
        {
            "origin": f"{sede['sede_lat']},{sede['sede_lon']}",
            "destination": f"{destination['lat']},{destination['lon']}",
            "key": api_key,
            "language": "es",
        }
    )
    with urlopen(f"{GOOGLE_DIRECTIONS_URL}?{query}", timeout=timeout_s) as response:
        payload = json.loads(response.read().decode("utf-8"))

    api_status = payload.get("status", "UNKNOWN_ERROR")
    routes = payload.get("routes", [])
    if api_status == "OK" and routes:
        leg = routes[0]["legs"][0]
        return {
            "distance_m": float(leg["distance"]["value"]),
            "duration_s": float(leg["duration"]["value"]),
            "routing_status": "ok",
            "api_status": api_status,
            "error_message": pd.NA,
        }
    if api_status == "ZERO_RESULTS":
        return {
            "distance_m": pd.NA,
            "duration_s": pd.NA,
            "routing_status": "zero_results",
            "api_status": api_status,
            "error_message": "Google Maps no devolvió ruta para el destino",
        }
    return {
        "distance_m": pd.NA,
        "duration_s": pd.NA,
        "routing_status": "api_error",
        "api_status": api_status,
        "error_message": payload.get("error_message", "Google Maps devolvió un estado no exitoso"),
    }


def _build_scope(destinos: pd.DataFrame, cache: pd.DataFrame) -> pd.DataFrame:
    scope = destinos.copy()

    cache_view = cache[
        [
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
    ]
    scope = scope.merge(cache_view, on=["sede_id", "destino_key"], how="left")
    scope["routing_status"] = scope["routing_status"].fillna("pending")
    scope["cache_version"] = scope["cache_version"].fillna(CACHE_VERSION)
    scope["fuente_ruteo"] = scope["fuente_ruteo"].fillna(GOOGLE_SOURCE)
    scope["selected_this_run"] = False
    return scope


def _write_report(path: Path, summary: dict[str, object]) -> None:
    lines = [
        "# Preparación de ruteo Google Maps — Phase 3",
        "",
        "## Supuestos activos",
        "",
        "- El ruteo distingue cada origen por `(sede_id, destino_key)` para evitar sesgo de sede única.",
        "- `agua_cloacas` se aplica a `Agua Potable`, `Agua Piray` y `Cloaca`.",
        "- `tv_internet` se aplica a `Transmision de Datos`, `TV Cable` y `TV Aire`.",
        "- `energia` conserva `Energia`, `Energia Prepaga`, `Alumbrado Publico` y los servicios no mapeados explícitamente.",
        "- El scope se deriva de `reclamos_clean.parquet`, no de una sede fija.",
        f"- `max_new_requests` aplicado en esta corrida: `{summary['max_new_requests']}`.",
        "",
        "## Resultados",
        "",
        f"- Pares `(sede_id, destino_key)` evaluados: `{summary['unique_destinations']}`.",
        f"- Reclamos cubiertos por esos destinos: `{summary['covered_claims']}`.",
        f"- Destinos `ok` en cache: `{summary['ok_cached']}`.",
        f"- Destinos `zero_results`: `{summary['zero_results']}`.",
        f"- Destinos con `api_error`: `{summary['api_error']}`.",
        f"- Destinos aún `pending`: `{summary['pending']}`.",
        f"- Nuevas llamadas realizadas en esta corrida: `{summary['attempted_this_run']}`.",
        f"- Estado de API detectado: `{summary['api_mode']}`.",
        "",
        "## Outputs generados",
        "",
        "- `data/intermediate/distancias_cache.parquet`",
        "- `data/intermediate/routing_scope.parquet`",
        "- `data/processed/sede_ref.parquet`",
        "- `data/processed/servicio_sede_ref.parquet`",
        "- `data/processed/costos_ref.parquet`",
    ]
    per_sede = summary.get("per_sede", [])
    if per_sede:
        lines.extend(["", "## Cobertura por sede", ""])
        for row in per_sede:
            lines.append(
                "- `{sede_id}`: pairs=`{pairs}`, reclamos=`{claims}`, ok=`{ok}`, pending=`{pending}`".format(
                    sede_id=row["sede_id"],
                    pairs=row["routing_pairs"],
                    claims=row["reclamos_count"],
                    ok=row["ok_cached"],
                    pending=row["pending"],
                )
            )
    if summary.get("fatal_api_status"):
        lines.extend(["", "## Bloqueo detectado", "", f"- Estado fatal: `{summary['fatal_api_status']}`."])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _load_batch_history(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    history = pd.read_parquet(path)
    if history.empty:
        return pd.DataFrame()
    return history.sort_values(["run_started_at", "batch_number"], kind="stable").reset_index(drop=True)


def _current_cache_summary() -> dict[str, int]:
    cache_path = INTERMEDIATE_DIR / "distancias_cache.parquet"
    cache = _load_existing_cache(cache_path)
    return {
        "unique_destinations": int(len(cache)),
        "ok_cached": int(cache["routing_status"].eq("ok").sum()),
        "zero_results": int(cache["routing_status"].eq("zero_results").sum()),
        "api_error": int(cache["routing_status"].eq("api_error").sum()),
        "request_error": int(cache["routing_status"].eq("request_error").sum()),
        "pending": int(cache["routing_status"].eq("pending").sum()),
    }


def _write_automation_report(
    path: Path,
    *,
    run_started_at: datetime,
    run_finished_at: datetime,
    batch_size: int,
    max_batches: int,
    max_runtime_minutes: int,
    batch_sleep_seconds: int,
    stop_reason: str,
    history: pd.DataFrame,
    current_summary: dict[str, int],
) -> None:
    elapsed_minutes = (run_finished_at - run_started_at).total_seconds() / 60.0
    lines = [
        "# Automatización de ruteo Google Maps — Phase 3.3",
        "",
        "## Presupuesto explícito",
        "",
        f"- `batch_size`: `{batch_size}` destinos.",
        f"- `max_batches`: `{max_batches}`.",
        f"- `max_runtime_minutes`: `{max_runtime_minutes}`.",
        f"- `batch_sleep_seconds`: `{batch_sleep_seconds}`.",
        f"- `run_started_at`: `{run_started_at.isoformat()}`.",
        f"- `run_finished_at`: `{run_finished_at.isoformat()}`.",
        f"- `elapsed_minutes`: `{elapsed_minutes:.2f}`.",
        f"- `stop_reason`: `{stop_reason}`.",
        "",
        "## Estado final del cache",
        "",
        f"- Destinos únicos: `{current_summary['unique_destinations']}`.",
        f"- Destinos `ok`: `{current_summary['ok_cached']}`.",
        f"- Destinos `zero_results`: `{current_summary['zero_results']}`.",
        f"- Destinos `api_error`: `{current_summary['api_error']}`.",
        f"- Destinos `request_error`: `{current_summary['request_error']}`.",
        f"- Destinos `pending`: `{current_summary['pending']}`.",
    ]
    if not history.empty:
        lines.extend(["", "## Historial por batch", ""])
        run_history = history.loc[history["run_started_at"] == run_started_at.isoformat()].copy()
        for _, row in run_history.iterrows():
            lines.append(
                "- Batch {batch}: attempted={attempted}, ok_total={ok_total}, pending_total={pending_total}, "
                "status={status}, stop_reason={batch_stop}.".format(
                    batch=int(row["batch_number"]),
                    attempted=int(row["attempted_this_run"]),
                    ok_total=int(row["ok_cached_total"]),
                    pending_total=int(row["pending_total"]),
                    status=row["batch_status"],
                    batch_stop=row["batch_stop_reason"],
                )
            )
    lines.extend(
        [
            "",
            "## Persistencia",
            "",
            "- Cada batch reescribe `data/intermediate/distancias_cache.parquet` y `data/intermediate/routing_scope.parquet`.",
            "- Cada batch refresca `data/processed/distancias_costos.parquet` y `data/processed/reclamos_enriquecidos.parquet`.",
            "- El historial acumulado queda en `data/intermediate/routing_batch_history.parquet`.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_google_maps_cache(
    max_new_requests: int = 25,
    timeout_s: int = 30,
    retry_failed: bool = False,
) -> dict[str, object]:
    ensure_workspace_layout()
    destinos = _load_routing_pairs()

    artifacts = RoutingArtifacts(
        distancias_cache=INTERMEDIATE_DIR / "distancias_cache.parquet",
        routing_scope=INTERMEDIATE_DIR / "routing_scope.parquet",
        report=REPORTS_DIR / "phase3_routing_prep.md",
    )

    existing_cache = _load_existing_cache(artifacts.distancias_cache, routing_pairs=destinos)
    scope = _build_scope(destinos, existing_cache)
    api_key = get_env_var("GCP_API_KEY")
    api_mode = "configured" if api_key else "missing_api_key"
    attempted_this_run = 0
    fatal_api_status: str | None = None

    if api_key:
        pending_mask = scope["routing_status"].isin(["pending"]) | (
            retry_failed & scope["routing_status"].isin(["api_error", "request_error"])
        )
        pending_scope = scope.loc[pending_mask].head(max_new_requests)
        for index, row in pending_scope.iterrows():
            requested_at = datetime.now(UTC).isoformat()
            try:
                route = _fetch_route(
                    api_key=api_key,
                    sede={"sede_lat": row["sede_lat"], "sede_lon": row["sede_lon"]},
                    destination=row,
                    timeout_s=timeout_s,
                )
            except HTTPError as exc:
                route = {
                    "distance_m": pd.NA,
                    "duration_s": pd.NA,
                    "routing_status": "request_error",
                    "api_status": f"HTTP_{exc.code}",
                    "error_message": str(exc),
                }
            except URLError as exc:
                route = {
                    "distance_m": pd.NA,
                    "duration_s": pd.NA,
                    "routing_status": "request_error",
                    "api_status": "URL_ERROR",
                    "error_message": str(exc.reason),
                }
            except Exception as exc:  # noqa: BLE001
                route = {
                    "distance_m": pd.NA,
                    "duration_s": pd.NA,
                    "routing_status": "request_error",
                    "api_status": "UNEXPECTED_ERROR",
                    "error_message": str(exc),
                }

            scope.loc[index, "distance_m"] = route["distance_m"]
            scope.loc[index, "duration_s"] = route["duration_s"]
            scope.loc[index, "routing_status"] = route["routing_status"]
            scope.loc[index, "api_status"] = route["api_status"]
            scope.loc[index, "error_message"] = route["error_message"]
            scope.loc[index, "requested_at"] = requested_at
            scope.loc[index, "cache_version"] = CACHE_VERSION
            scope.loc[index, "fuente_ruteo"] = GOOGLE_SOURCE
            scope.loc[index, "selected_this_run"] = True
            attempted_this_run += 1

            if route["api_status"] in FATAL_API_STATUSES:
                fatal_api_status = str(route["api_status"])
                api_mode = "fatal_api_error"
                break
    else:
        scope["api_status"] = scope["api_status"].fillna("MISSING_API_KEY")
        scope["error_message"] = scope["error_message"].fillna("No se encontró GCP_API_KEY en ceia-amq-tp/.env")

    cache = scope[
        [
            "sede_id",
            "sede_nombre",
            "sede_lat",
            "sede_lon",
            "destino_key",
            "lat",
            "lon",
            "reclamos_count",
            "servicios_count",
            "servicios",
            "distance_m",
            "duration_s",
            "routing_status",
            "fuente_ruteo",
            "api_status",
            "error_message",
            "requested_at",
            "cache_version",
        ]
    ].copy()
    cache.to_parquet(artifacts.distancias_cache, index=False)
    scope.to_parquet(artifacts.routing_scope, index=False)

    per_sede = (
        scope.groupby(["sede_id", "sede_nombre"], dropna=False)
        .agg(
            routing_pairs=("destino_key", "size"),
            reclamos_count=("reclamos_count", "sum"),
            ok_cached=("routing_status", lambda series: int(series.eq("ok").sum())),
            pending=("routing_status", lambda series: int(series.eq("pending").sum())),
        )
        .reset_index()
        .sort_values(["routing_pairs", "sede_id"], ascending=[False, True], kind="stable")
    )

    summary = {
        "unique_destinations": int(len(scope)),
        "covered_claims": int(scope["reclamos_count"].sum()),
        "ok_cached": int((scope["routing_status"] == "ok").sum()),
        "zero_results": int((scope["routing_status"] == "zero_results").sum()),
        "api_error": int((scope["routing_status"] == "api_error").sum()),
        "pending": int((scope["routing_status"] == "pending").sum()),
        "request_error": int((scope["routing_status"] == "request_error").sum()),
        "attempted_this_run": int(attempted_this_run),
        "max_new_requests": int(max_new_requests),
        "api_mode": api_mode,
        "fatal_api_status": fatal_api_status,
        "per_sede": per_sede.to_dict(orient="records"),
    }
    _write_report(artifacts.report, summary=summary)

    return {
        "summary": summary,
        "artifacts": {field: str(getattr(artifacts, field)) for field in artifacts.__dataclass_fields__},
    }


def run_routing_automation(
    *,
    batch_size: int = 250,
    max_batches: int = 4,
    max_runtime_minutes: int = 20,
    batch_sleep_seconds: int = 0,
    timeout_s: int = 30,
    retry_failed: bool = False,
    refresh_aggregates: bool = True,
) -> dict[str, object]:
    ensure_workspace_layout()
    automation_artifacts = RoutingAutomationArtifacts(
        batch_history=INTERMEDIATE_DIR / "routing_batch_history.parquet",
        report=REPORTS_DIR / "phase3_routing_automation.md",
    )
    existing_history = _load_batch_history(automation_artifacts.batch_history)

    run_started_at = datetime.now(UTC)
    deadline = run_started_at + timedelta(minutes=max_runtime_minutes)
    run_id = run_started_at.strftime("%Y%m%dT%H%M%SZ")
    reference_result = write_reference_outputs()
    last_batch: dict[str, object] | None = None
    stop_reason = "max_batches_reached"
    batch_records: list[dict[str, object]] = []

    for batch_number in range(1, max_batches + 1):
        if datetime.now(UTC) >= deadline:
            stop_reason = "time_budget_exhausted"
            break

        batch_started_at = datetime.now(UTC)
        batch_status = "success"
        batch_stop_reason = "continue"
        batch_error_message: str | None = None
        aggregates_result: dict[str, object] | None = None

        try:
            routing_result = build_google_maps_cache(
                max_new_requests=batch_size,
                timeout_s=timeout_s,
                retry_failed=retry_failed,
            )
            costing_result = build_costing_outputs()
            if refresh_aggregates:
                aggregates_result = build_operational_aggregates()
            last_batch = {
                "batch_number": batch_number,
                "routing_result": routing_result,
                "costing_result": costing_result,
                "aggregates_result": aggregates_result,
            }
            routing_summary = routing_result["summary"]
            if routing_summary["api_mode"] == "missing_api_key":
                batch_status = "stopped"
                batch_stop_reason = "missing_api_key"
                stop_reason = batch_stop_reason
            elif routing_summary["fatal_api_status"]:
                batch_status = "stopped"
                batch_stop_reason = f"fatal_api_status:{routing_summary['fatal_api_status']}"
                stop_reason = batch_stop_reason
            elif routing_summary["attempted_this_run"] == 0 and routing_summary["pending"] == 0:
                batch_status = "stopped"
                batch_stop_reason = "all_pending_resolved"
                stop_reason = batch_stop_reason
            elif routing_summary["attempted_this_run"] == 0:
                batch_status = "stopped"
                batch_stop_reason = "no_progress_possible"
                stop_reason = batch_stop_reason
        except Exception as exc:  # noqa: BLE001
            batch_status = "recoverable_failure"
            batch_stop_reason = "terminated_safely_after_error"
            batch_error_message = str(exc)
            stop_reason = batch_stop_reason
            routing_summary = _current_cache_summary()
            costing_result = build_costing_outputs()
            if refresh_aggregates:
                aggregates_result = build_operational_aggregates()
            last_batch = {
                "batch_number": batch_number,
                "routing_result": {"summary": routing_summary, "artifacts": {}},
                "costing_result": costing_result,
                "aggregates_result": aggregates_result,
            }

        batch_finished_at = datetime.now(UTC)
        current_summary = _current_cache_summary()
        batch_records.append(
            {
                "run_id": run_id,
                "run_started_at": run_started_at.isoformat(),
                "batch_number": batch_number,
                "batch_started_at": batch_started_at.isoformat(),
                "batch_finished_at": batch_finished_at.isoformat(),
                "elapsed_seconds": round((batch_finished_at - batch_started_at).total_seconds(), 3),
                "attempted_this_run": int(last_batch["routing_result"]["summary"].get("attempted_this_run", 0)) if last_batch else 0,
                "ok_cached_total": int(current_summary["ok_cached"]),
                "pending_total": int(current_summary["pending"]),
                "zero_results_total": int(current_summary["zero_results"]),
                "api_error_total": int(current_summary["api_error"]),
                "request_error_total": int(current_summary["request_error"]),
                "batch_status": batch_status,
                "batch_stop_reason": batch_stop_reason,
                "error_message": batch_error_message,
            }
        )

        history = pd.concat([existing_history, pd.DataFrame(batch_records)], ignore_index=True)
        history.to_parquet(automation_artifacts.batch_history, index=False)
        _write_automation_report(
            automation_artifacts.report,
            run_started_at=run_started_at,
            run_finished_at=batch_finished_at,
            batch_size=batch_size,
            max_batches=max_batches,
            max_runtime_minutes=max_runtime_minutes,
            batch_sleep_seconds=batch_sleep_seconds,
            stop_reason=stop_reason if batch_stop_reason != "continue" else "running",
            history=history,
            current_summary=current_summary,
        )

        if batch_stop_reason != "continue":
            break

        next_batch_number = batch_number + 1
        if batch_sleep_seconds > 0 and next_batch_number <= max_batches:
            remaining_seconds = (deadline - datetime.now(UTC)).total_seconds()
            if remaining_seconds <= 0:
                stop_reason = "time_budget_exhausted"
                break
            time.sleep(min(float(batch_sleep_seconds), max(0.0, remaining_seconds)))

    run_finished_at = datetime.now(UTC)
    final_history = _load_batch_history(automation_artifacts.batch_history)
    final_summary = _current_cache_summary()
    _write_automation_report(
        automation_artifacts.report,
        run_started_at=run_started_at,
        run_finished_at=run_finished_at,
        batch_size=batch_size,
        max_batches=max_batches,
        max_runtime_minutes=max_runtime_minutes,
        batch_sleep_seconds=batch_sleep_seconds,
        stop_reason=stop_reason,
        history=final_history,
        current_summary=final_summary,
    )

    return {
        "run_id": run_id,
        "summary": {
            "batch_size": batch_size,
            "max_batches": max_batches,
            "max_runtime_minutes": max_runtime_minutes,
            "batch_sleep_seconds": batch_sleep_seconds,
            "stop_reason": stop_reason,
            **final_summary,
        },
        "reference_result": reference_result,
        "last_batch": last_batch,
        "artifacts": {field: str(getattr(automation_artifacts, field)) for field in automation_artifacts.__dataclass_fields__},
    }


if __name__ == "__main__":
    print(pd.Series(build_google_maps_cache()["summary"]).to_string())
