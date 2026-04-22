from __future__ import annotations

import json
import unicodedata
from pathlib import Path

import pandas as pd

from helpers.contracts import validate_processed_contract
from helpers.paths import PROCESSED_DIR, require_input


ENERGIA_SEDE_ID = "energia"
AGUA_CLOACAS_SEDE_ID = "agua_cloacas"
TV_INTERNET_SEDE_ID = "tv_internet"

SEDE_COORDINATE_OVERRIDES: dict[str, tuple[float, float]] = {
    ENERGIA_SEDE_ID: (-26.56546259837734, -54.75411204580389),
    AGUA_CLOACAS_SEDE_ID: (-26.582934060558625, -54.73012469788584),
    TV_INTERNET_SEDE_ID: (-26.568394449145398, -54.754005706687934),
}

EXPLICIT_SERVICE_TO_SEDE: dict[str, str] = {
    "AGUA POTABLE": AGUA_CLOACAS_SEDE_ID,
    "AGUA PIRAY": AGUA_CLOACAS_SEDE_ID,
    "CLOACA": AGUA_CLOACAS_SEDE_ID,
    "TRANSMISION DE DATOS": TV_INTERNET_SEDE_ID,
    "TV CABLE": TV_INTERNET_SEDE_ID,
    "TV AIRE": TV_INTERNET_SEDE_ID,
    "ENERGIA": ENERGIA_SEDE_ID,
    "ENERGIA PREPAGA": ENERGIA_SEDE_ID,
    "ALUMBRADO PUBLICO": ENERGIA_SEDE_ID,
}


def _load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_service_name(value: object) -> str:
    if pd.isna(value):
        return "SIN_SERVICIO"
    text = str(value).strip()
    if not text:
        return "SIN_SERVICIO"
    normalized = unicodedata.normalize("NFKD", text)
    normalized = "".join(character for character in normalized if not unicodedata.combining(character))
    normalized = " ".join(normalized.upper().split())
    return normalized or "SIN_SERVICIO"


def build_destino_key(lat_series: pd.Series, lon_series: pd.Series) -> pd.Series:
    return lat_series.round(5).map(lambda value: f"{value:.5f}") + "_" + lon_series.round(5).map(lambda value: f"{value:.5f}")


def load_sede_ref() -> pd.DataFrame:
    sedes_payload = _load_json(require_input("sedes_servicios"))
    sedes = pd.DataFrame(sedes_payload["sedes"]).copy()
    if sedes.empty:
        raise ValueError("sedes_servicios.json no contiene sedes")

    sedes = sedes.rename(columns={"id": "sede_id", "nombre": "sede_nombre"})
    for sede_id, (expected_lat, expected_lon) in SEDE_COORDINATE_OVERRIDES.items():
        mask = sedes["sede_id"] == sede_id
        if not mask.any():
            raise ValueError(f"No existe la sede requerida `{sede_id}` en sedes_servicios.json")
        sedes.loc[mask, "lat"] = expected_lat
        sedes.loc[mask, "lon"] = expected_lon

    sede_ref = sedes[["sede_id", "sede_nombre", "lat", "lon", "direccion", "telefono"]].copy()
    sede_ref["lat"] = pd.to_numeric(sede_ref["lat"], errors="raise")
    sede_ref["lon"] = pd.to_numeric(sede_ref["lon"], errors="raise")
    validate_processed_contract(sede_ref[["sede_id", "sede_nombre", "lat", "lon"]], "sede_ref")
    return sede_ref.sort_values("sede_id", kind="stable").reset_index(drop=True)


def _mapping_candidates_from_json() -> list[dict[str, object]]:
    sedes_payload = _load_json(require_input("sedes_servicios"))
    rows: list[dict[str, object]] = []
    for sede in sedes_payload["sedes"]:
        sede_id = str(sede["id"])
        for service in sede.get("servicios", []):
            rows.append(
                {
                    "servicio": str(service),
                    "servicio_normalizado": normalize_service_name(service),
                    "sede_id": sede_id,
                    "mapping_source": "sedes_servicios.json",
                }
            )
    return rows


def build_service_sede_ref(observed_services: pd.Series | None = None) -> pd.DataFrame:
    sede_ref = load_sede_ref()[["sede_id", "sede_nombre", "lat", "lon"]]
    rows = _mapping_candidates_from_json()
    for service_name, sede_id in EXPLICIT_SERVICE_TO_SEDE.items():
        rows.append(
            {
                "servicio": service_name.title(),
                "servicio_normalizado": normalize_service_name(service_name),
                "sede_id": sede_id,
                "mapping_source": "explicit_override",
            }
        )

    service_ref = pd.DataFrame(rows)
    service_ref = service_ref.sort_values(["servicio_normalizado", "mapping_source"], ascending=[True, False], kind="stable")
    service_ref = service_ref.drop_duplicates(subset=["servicio_normalizado"], keep="first")

    if observed_services is not None:
        observed_frame = pd.DataFrame({"servicio": observed_services.fillna("SIN_SERVICIO").astype(str).str.strip()})
        observed_frame["servicio_normalizado"] = observed_frame["servicio"].map(normalize_service_name)
        observed_frame = observed_frame.drop_duplicates(subset=["servicio_normalizado"], keep="first")
        service_ref = observed_frame.merge(
            service_ref[["servicio_normalizado", "sede_id", "mapping_source"]],
            on="servicio_normalizado",
            how="left",
        )
        service_ref["sede_id"] = service_ref["sede_id"].fillna(ENERGIA_SEDE_ID)
        service_ref["mapping_source"] = service_ref["mapping_source"].fillna("default_energia_fallback")
    else:
        service_ref = service_ref[["servicio", "servicio_normalizado", "sede_id", "mapping_source"]]

    service_ref = service_ref.merge(sede_ref, on="sede_id", how="left", validate="many_to_one")
    validate_processed_contract(service_ref[["servicio", "servicio_normalizado", "sede_id"]], "servicio_sede_ref")
    return service_ref.sort_values(["sede_id", "servicio_normalizado"], kind="stable").reset_index(drop=True)


def assign_sede_by_service(reclamos: pd.DataFrame, service_column: str = "servicio") -> pd.DataFrame:
    if service_column not in reclamos.columns:
        raise KeyError(f"No existe la columna de servicio `{service_column}` en el dataframe provisto")

    service_ref = build_service_sede_ref(reclamos[service_column])
    enriched = reclamos.copy()
    enriched["servicio_normalizado"] = enriched[service_column].map(normalize_service_name)
    enriched = enriched.merge(
        service_ref[["servicio_normalizado", "sede_id", "sede_nombre", "lat", "lon", "mapping_source"]].rename(
            columns={"lat": "sede_lat", "lon": "sede_lon", "mapping_source": "servicio_sede_mapping_source"}
        ),
        on="servicio_normalizado",
        how="left",
        validate="many_to_one",
    )
    enriched["sede_id"] = enriched["sede_id"].fillna(ENERGIA_SEDE_ID)
    enriched["servicio_sede_mapping_source"] = enriched["servicio_sede_mapping_source"].fillna("default_energia_fallback")
    return enriched


def build_routing_scope_pairs(reclamos: pd.DataFrame) -> pd.DataFrame:
    enriched = assign_sede_by_service(reclamos)
    enriched["lat"] = pd.to_numeric(enriched["lat"], errors="raise").round(5)
    enriched["lon"] = pd.to_numeric(enriched["lon"], errors="raise").round(5)
    enriched["destino_key"] = build_destino_key(enriched["lat"], enriched["lon"])

    grouped = (
        enriched.groupby(["sede_id", "sede_nombre", "sede_lat", "sede_lon", "destino_key", "lat", "lon"], dropna=False)
        .agg(
            reclamos_count=("reclamo_id", "nunique"),
            servicios_count=("servicio_normalizado", "nunique"),
            servicios=("servicio", lambda series: " | ".join(sorted({str(value).strip() for value in series if str(value).strip()}))),
        )
        .reset_index()
        .sort_values(["reclamos_count", "sede_id", "destino_key"], ascending=[False, True, True], kind="stable")
        .reset_index(drop=True)
    )
    grouped["priority_rank"] = range(1, len(grouped) + 1)
    return grouped


def load_observed_service_sede_ref() -> pd.DataFrame:
    reclamos_path = PROCESSED_DIR / "reclamos_clean.parquet"
    if reclamos_path.exists():
        reclamos = pd.read_parquet(reclamos_path, columns=["servicio"])
        return build_service_sede_ref(reclamos["servicio"])
    return build_service_sede_ref()
