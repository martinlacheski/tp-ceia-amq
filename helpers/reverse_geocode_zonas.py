"""
Reverse Geocoding para las 43 zonas clusterizadas.

Uso:
    python -m helpers.reverse_geocode_zonas

Genera:
    data/processed/zonas_geocoded.parquet
    data/processed/zonas_geocoded_report.md
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd
import urllib.error
import urllib.parse
import urllib.request

from helpers.paths import PROCESSED_DIR, REPORTS_DIR, get_env_var, ensure_workspace_layout


GEOCODING_URL = "https://maps.googleapis.com/maps/api/geocode/json"
CACHE_VERSION = "v2"


def _geocode(lat: float, lon: float, api_key: str, timeout_s: int = 10) -> dict:
    """Single reverse geocode request."""
    params = urllib.parse.urlencode({
        "latlng": f"{lat},{lon}",
        "key": api_key,
        "result_type": "locality|sublocality|neighborhood|route",
        "language": "es",
    })
    url = f"{GEOCODING_URL}?{params}"

    with urllib.request.urlopen(url, timeout=timeout_s) as response:
        return json.loads(response.read().decode("utf-8"))


def _extract_components(results: list) -> dict:
    """Extrae los componentes de dirección más relevantes.

    Estrategia: buscar 'locality' en CUALQUIER resultado, no solo el primero.
    Esto evita perder datos cuando el primer resultado es un plus code.
    """
    if not results:
        return {"formatted_address": None, "locality": None, "sublocality": None, "neighborhood": None}

    locality = sublocality = neighborhood = None
    formatted = results[0].get("formatted_address")

    for result in results:
        for comp in result.get("address_components", []):
            types = comp.get("types", [])
            long_name = comp.get("long_name", "")

            if not locality and "locality" in types:
                locality = long_name
            elif not sublocality and "sublocality" in types:
                sublocality = long_name
            elif not neighborhood and "neighborhood" in types:
                neighborhood = long_name

        if locality:
            break

    return {
        "formatted_address": formatted,
        "locality": locality,
        "sublocality": sublocality,
        "neighborhood": neighborhood,
    }


def _resolve_zone_name(row: dict) -> str:
    """Determina el nombre más representativo de la zona."""
    return row.get("locality") or row.get("sublocality") or row.get("neighborhood") or "Unknown"


def rerun_unresolved() -> dict:
    """Re-procesa solo las zonas con 'Unknown' usando lógica mejorada."""
    ensure_workspace_layout()

    zonas = pd.read_parquet(PROCESSED_DIR / "zona_cluster_resumen.parquet")
    existing = pd.read_parquet(PROCESSED_DIR / "zonas_geocoded.parquet")

    # Filtrar solo unknowns
    unknown_mask = existing["zone_name"] == "Unknown"
    unknown_zonas = existing[unknown_mask].copy()

    if unknown_zonas.empty:
        return {"message": "No hay zonas unknown para re-procesar", "rerun": 0}

    api_key = get_env_var("GCP_API_KEY") or get_env_var("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError("No se encontró GCP_API_KEY ni GOOGLE_API_KEY en .env")

    output_path = PROCESSED_DIR / "zonas_geocoded.parquet"

    updates = []
    for idx, row in unknown_zonas.iterrows():
        zona_id = row["zona_id"]
        lat = row["centroid_lat"]
        lon = row["centroid_lon"]

        print(f"[rerun] {zona_id}: {lat}, {lon}")

        try:
            params = urllib.parse.urlencode({
                "latlng": f"{lat},{lon}",
                "key": api_key,
                "language": "es",
            })
            url = f"{GEOCODING_URL}?{params}"

            with urllib.request.urlopen(url, timeout=10) as response:
                payload = json.loads(response.read().decode("utf-8"))

            status = payload.get("status", "UNKNOWN")
            if status == "OK" and payload.get("results"):
                extracted = _extract_components(payload["results"])
                zone_name = _resolve_zone_name(extracted)
            else:
                extracted = {"formatted_address": None, "locality": None, "sublocality": None, "neighborhood": None}
                zone_name = f"{zona_id}_unresolved"

            updates.append({
                "zona_id": zona_id,
                "geocode_status": status,
                **extracted,
                "zone_name": zone_name,
            })

        except Exception as exc:
            print(f"  ❌ {type(exc).__name__}: {exc}")
            updates.append({
                "zona_id": zona_id,
                "geocode_status": "ERROR",
                "formatted_address": None,
                "locality": None,
                "sublocality": None,
                "neighborhood": None,
                "zone_name": f"{zona_id}_error",
            })

        time.sleep(0.05)

    # Merge actualizaciones
    updates_df = pd.DataFrame(updates)
    merged = existing.copy()
    for _, update in updates_df.iterrows():
        mask = merged["zona_id"] == update["zona_id"]
        for col in ["geocode_status", "formatted_address", "locality", "sublocality", "neighborhood", "zone_name"]:
            merged.loc[mask, col] = update[col]

    merged.to_parquet(output_path, index=False)

    ok_after = (merged["zone_name"] != "Unknown").sum()
    print(f"\n✅ Actualizado: {output_path}")
    print(f"   Zonas resueltas: {ok_after}/43")

    return {
        "total": len(merged),
        "resolved_after_rerun": int(ok_after - (existing["zone_name"] != "Unknown").sum()),
        "ok": ok_after,
        "unknown_remaining": int((merged["zone_name"] == "Unknown").sum()),
        "output": str(output_path),
    }


def run() -> dict:
    ensure_workspace_layout()

    # Cargar zonas clusterizadas
    zonas = pd.read_parquet(PROCESSED_DIR / "zona_cluster_resumen.parquet")
    zonas = zonas.sort_values("zona_id").reset_index(drop=True)

    api_key = get_env_var("GCP_API_KEY") or get_env_var("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError("No se encontró GCP_API_KEY ni GOOGLE_API_KEY en .env")

    output_path = PROCESSED_DIR / "zonas_geocoded.parquet"
    report_path = REPORTS_DIR / "zonas_geocoded_report.md"

    results = []
    errors = 0

    for idx, row in zonas.iterrows():
        zona_id = row["zona_id"]
        lat = row["centroid_lat"]
        lon = row["centroid_lon"]

        print(f"[{idx + 1}/{len(zonas)}] {zona_id}: {lat}, {lon}")

        try:
            payload = _geocode(lat, lon, api_key)
            status = payload.get("status", "UNKNOWN")

            if status == "OK" and payload.get("results"):
                extracted = _extract_components(payload["results"])
                zone_name = _resolve_zone_name(extracted)
                errors = 0
            else:
                extracted = {"formatted_address": None, "locality": None, "sublocality": None, "neighborhood": None}
                zone_name = f"{zona_id}_unresolved"
                print(f"  ⚠ status={status}")

            results.append({
                "zona_id": zona_id,
                "centroid_lat": lat,
                "centroid_lon": lon,
                "reclamos_count": row["reclamos_count"],
                "cluster_root": row["cluster_root"],
                "geocode_status": status,
                **extracted,
                "zone_name": zone_name,
                "cache_version": CACHE_VERSION,
            })

        except urllib.error.HTTPError as exc:
            print(f"  ❌ HTTP {exc.code}: {exc.reason}")
            results.append({
                "zona_id": zona_id,
                "centroid_lat": lat,
                "centroid_lon": lon,
                "reclamos_count": row["reclamos_count"],
                "cluster_root": row["cluster_root"],
                "geocode_status": f"HTTP_{exc.code}",
                "formatted_address": None,
                "locality": None,
                "sublocality": None,
                "neighborhood": None,
                "zone_name": f"{zona_id}_error",
                "cache_version": CACHE_VERSION,
            })
        except Exception as exc:
            print(f"  ❌ {type(exc).__name__}: {exc}")
            results.append({
                "zona_id": zona_id,
                "centroid_lat": lat,
                "centroid_lon": lon,
                "reclamos_count": row["reclamos_count"],
                "cluster_root": row["cluster_root"],
                "geocode_status": "ERROR",
                "formatted_address": None,
                "locality": None,
                "sublocality": None,
                "neighborhood": None,
                "zone_name": f"{zona_id}_error",
                "cache_version": CACHE_VERSION,
            })

        # Rate limiting: 50ms entre requests (~20req/s, bien debajo del límite)
        time.sleep(0.05)

    # Armar DataFrame y guardar
    df = pd.DataFrame(results)
    df.to_parquet(output_path, index=False)

    # Reporte
    report_lines = [
        "# Reverse Geocoding de Zonas — Reporte",
        "",
        f"Zonas procesadas: {len(df)}",
        f"Geocoding exitoso: {(df['geocode_status'] == 'OK').sum()}",
        f"Con errors: {(df['geocode_status'] != 'OK').sum()}",
        "",
        "## Resultados",
        "",
    ]

    for _, row in df.iterrows():
        status_icon = "✅" if row["geocode_status"] == "OK" else "⚠️"
        report_lines.append(
            f"{status_icon} **{row['zone_name']}** (`{row['zona_id']}`) — "
            f"{row['reclamos_count']:,} reclamos, "
            f"coords: ({row['centroid_lat']:.4f}, {row['centroid_lon']:.4f})"
        )

    report_path.write_text("\n".join(report_lines), encoding="utf-8")
    print(f"\n✅ Guardado: {output_path}")
    print(f"✅ Reporte: {report_path}")

    return {
        "total": len(df),
        "ok": int((df["geocode_status"] == "OK").sum()),
        "errored": int((df["geocode_status"] != "OK").sum()),
        "output": str(output_path),
        "report": str(report_path),
    }


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "rerun-unresolved":
        print("=== Re-procesando zonas Unknown ===\n")
        result = rerun_unresolved()
        print("\nResumen:", result)
    else:
        print("=== Reverse Geocoding de Zonas ===\n")
        result = run()
        print("\nResumen:", result)