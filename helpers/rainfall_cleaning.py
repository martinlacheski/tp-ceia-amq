from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from helpers.contracts import validate_processed_contract
from helpers.paths import INTERMEDIATE_DIR, PROCESSED_DIR, RAW_RAIN_DIR, REPORTS_DIR, ensure_workspace_layout, require_input


DATE_WINDOW_START = pd.Timestamp("2021-01-01")
DATE_WINDOW_END = pd.Timestamp("2026-03-31")
DATASET_NAME = "lluvias_diarias"
MANUAL_RECOVERY_FILENAME = "rainfall_manual_recovered_months.csv"
MONTH_MAP = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}


@dataclass(slots=True)
class Phase3RainfallArtifacts:
    lluvia_diaria_clean: Path
    exclusiones_lluvia: Path
    lluvia_diaria_observada: Path
    report: Path


def _normalize_month_name(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def _load_rainfall_source() -> tuple[pd.DataFrame, dict[str, object]]:
    rainfall_path = require_input("lluvias")
    base_df = pd.read_csv(rainfall_path)
    metadata: dict[str, object] = {
        "base_input": str(rainfall_path),
        "manual_recovery_input": None,
        "manual_rows_loaded": 0,
        "manual_rows_applied": 0,
        "manual_documents": [],
    }

    manual_path = RAW_RAIN_DIR / MANUAL_RECOVERY_FILENAME
    if not manual_path.exists():
        return base_df, metadata

    manual_df = pd.read_csv(manual_path)
    manual_df = manual_df.reindex(columns=base_df.columns)
    metadata["manual_recovery_input"] = str(manual_path)
    metadata["manual_rows_loaded"] = int(len(manual_df))
    metadata["manual_documents"] = sorted(manual_df["archivo"].dropna().astype(str).unique().tolist())

    dedupe_key = ["archivo", "año_extraido", "dia"]
    base_with_priority = base_df.assign(_manual_priority=0)
    manual_with_priority = manual_df.assign(_manual_priority=1)
    combined = pd.concat([base_with_priority, manual_with_priority], ignore_index=True)
    deduped = (
        combined.sort_values(dedupe_key + ["_manual_priority"], kind="stable")
        .drop_duplicates(subset=dedupe_key, keep="last")
        .drop(columns="_manual_priority")
        .reset_index(drop=True)
    )
    metadata["manual_rows_applied"] = int(len(deduped) - len(base_df))
    return deduped, metadata


def _prepare_rainfall(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    work = df.copy()
    work["row_key"] = [f"{DATASET_NAME}:{index}" for index in work.index]
    work["dataset"] = DATASET_NAME
    work["mes_normalizado"] = work["mes_extraido"].map(_normalize_month_name)
    work["month_num"] = work["mes_normalizado"].map(MONTH_MAP)
    work["year_num"] = pd.to_numeric(work["año_extraido"], errors="coerce")
    work["day_num"] = pd.to_numeric(work["dia"], errors="coerce")
    work["lluvia_mm_num"] = pd.to_numeric(work["lluvia_mm"], errors="coerce")
    work["observacion_codigo"] = work["observacion_codigo"].fillna("").astype(str).str.strip()
    work["fecha"] = pd.to_datetime(
        {
            "year": work["year_num"],
            "month": work["month_num"],
            "day": work["day_num"],
        },
        errors="coerce",
    )

    exclusion_reason = pd.Series(pd.NA, index=work.index, dtype="object")
    exclusion_detail = pd.Series(pd.NA, index=work.index, dtype="object")

    invalid_month_mask = work["month_num"].isna()
    exclusion_reason.loc[invalid_month_mask] = "mes_invalido"
    exclusion_detail.loc[invalid_month_mask] = "mes_extraido no pudo mapearse a un mes calendario válido"

    invalid_date_mask = work["fecha"].isna() & exclusion_reason.isna()
    exclusion_reason.loc[invalid_date_mask] = "fecha_invalida"
    exclusion_detail.loc[invalid_date_mask] = "Combinación año/mes/día imposible después de la normalización"

    invalid_rain_mask = work["lluvia_mm_num"].isna() & exclusion_reason.isna()
    exclusion_reason.loc[invalid_rain_mask] = "lluvia_no_numerica"
    exclusion_detail.loc[invalid_rain_mask] = "lluvia_mm no pudo coercionarse a número"

    negative_rain_mask = (work["lluvia_mm_num"] < 0) & exclusion_reason.isna()
    exclusion_reason.loc[negative_rain_mask] = "lluvia_negativa"
    exclusion_detail.loc[negative_rain_mask] = "No se admiten precipitaciones negativas"

    out_of_range_mask = work["fecha"].notna() & ~work["fecha"].between(DATE_WINDOW_START, DATE_WINDOW_END, inclusive="both")
    exclusion_reason.loc[exclusion_reason.isna() & out_of_range_mask] = "fecha_fuera_rango_modelado"
    exclusion_detail.loc[exclusion_reason == "fecha_fuera_rango_modelado"] = (
        f"Se conserva solo la ventana {DATE_WINDOW_START.date()} → {DATE_WINDOW_END.date()}"
    )

    exclusions = work.loc[exclusion_reason.notna()].copy()
    exclusions["motivo_exclusion"] = exclusion_reason.loc[exclusion_reason.notna()]
    exclusions["detalle_exclusion"] = exclusion_detail.loc[exclusion_reason.notna()]
    exclusions = exclusions[
        [
            "row_key",
            "dataset",
            "archivo",
            "año_extraido",
            "mes_extraido",
            "dia",
            "lluvia_mm",
            "observacion_codigo",
            "motivo_exclusion",
            "detalle_exclusion",
        ]
    ].sort_values(["motivo_exclusion", "row_key"], kind="stable")

    valid = work.loc[exclusion_reason.isna()].copy()
    valid["fecha"] = valid["fecha"].dt.normalize()
    valid["observacion_codigo"] = valid["observacion_codigo"].replace("", pd.NA)

    observed = (
        valid.groupby("fecha", as_index=False)
        .agg(
            lluvia_mm=("lluvia_mm_num", "max"),
            observacion_codigo=(
                "observacion_codigo",
                lambda values: " | ".join(sorted({str(value) for value in values.dropna() if str(value).strip()})) or pd.NA,
            ),
            source_row_count=("row_key", "count"),
            source_file_count=("archivo", "nunique"),
        )
        .sort_values("fecha", kind="stable")
    )
    observed["lluvia_status"] = "observed"
    duplicate_mask = observed["source_row_count"] > 1
    observed.loc[duplicate_mask, "lluvia_status"] = "duplicate_collapsed"

    calendar = pd.DataFrame({"fecha": pd.date_range(DATE_WINDOW_START, DATE_WINDOW_END, freq="D")})
    clean = calendar.merge(observed, on="fecha", how="left")
    clean["lluvia_status"] = clean["lluvia_status"].fillna("missing_source")
    clean["source_row_count"] = clean["source_row_count"].fillna(0).astype(int)
    clean["source_file_count"] = clean["source_file_count"].fillna(0).astype(int)

    validate_processed_contract(clean[["fecha", "lluvia_mm", "lluvia_status"]], "lluvia_diaria")

    summary = {
        "rain_rows_raw": int(len(work)),
        "rain_rows_excluded": int(len(exclusions)),
        "rain_days_observed": int(len(observed)),
        "rain_days_calendar": int(len(clean)),
        "rain_days_missing_source": int((clean["lluvia_status"] == "missing_source").sum()),
        "rain_days_duplicate_collapsed": int((clean["lluvia_status"] == "duplicate_collapsed").sum()),
    }
    return clean, exclusions, observed, summary


def _write_rainfall_report(
    report_path: Path,
    summary: dict[str, int],
    exclusions: pd.DataFrame,
    source_metadata: dict[str, object],
) -> None:
    exclusion_counts = exclusions["motivo_exclusion"].value_counts().sort_index()
    manual_documents = source_metadata.get("manual_documents", [])
    lines = [
        "# Auditoría de lluvia diaria — Phase 3",
        "",
        "## Regla de normalización",
        "",
        f"- Ventana retenida: `{DATE_WINDOW_START.date()}` → `{DATE_WINDOW_END.date()}` inclusive.",
        "- La fecha final se construye desde `año_extraido` + `mes_extraido` + `dia`.",
        "- Se excluyen meses inválidos, fechas imposibles, lluvia no numérica/negativa y filas fuera de rango.",
        "- `lluvia_diaria_clean.parquet` queda alineado a calendario completo; los huecos se marcan `missing_source` sin inventar `lluvia_mm`.",
        "",
        "## Resultados",
        "",
        f"- Filas crudas analizadas: `{summary['rain_rows_raw']}`.",
        f"- Filas excluidas a auditoría: `{summary['rain_rows_excluded']}`.",
        f"- Días observados dentro del rango: `{summary['rain_days_observed']}`.",
        f"- Días calendario persistidos: `{summary['rain_days_calendar']}`.",
        f"- Días sin observación (`missing_source`): `{summary['rain_days_missing_source']}`.",
        f"- Días con duplicados colapsados: `{summary['rain_days_duplicate_collapsed']}`.",
        "",
        "## Fuente reproducible",
        "",
        f"- Input OCR base: `{source_metadata['base_input']}`.",
        f"- Input manual de recuperación: `{source_metadata['manual_recovery_input'] or 'no usado'}`.",
        f"- Filas manuales cargadas: `{int(source_metadata['manual_rows_loaded'])}`.",
        f"- Filas efectivamente aplicadas sobre el input base: `{int(source_metadata['manual_rows_applied'])}`.",
    ]
    if manual_documents:
        lines.append(f"- Documentos recuperados manualmente: `{', '.join(str(value) for value in manual_documents)}`.")
    lines.extend(
        [
            "",
        "## Exclusiones por motivo",
        "",
        ]
    )
    for reason, count in exclusion_counts.items():
        lines.append(f"- `{reason}`: `{int(count)}`")
    lines.extend(
        [
            "",
            "## Outputs generados",
            "",
            f"- `data/raw/rain/{MANUAL_RECOVERY_FILENAME}`",
            "- `data/processed/lluvia_diaria_clean.parquet`",
            "- `data/intermediate/exclusiones_lluvia.parquet`",
            "- `data/intermediate/lluvia_diaria_observada.parquet`",
        ]
    )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_rainfall_pipeline() -> dict[str, object]:
    ensure_workspace_layout()
    rainfall_df, source_metadata = _load_rainfall_source()

    clean, exclusions, observed, summary = _prepare_rainfall(rainfall_df)
    artifacts = Phase3RainfallArtifacts(
        lluvia_diaria_clean=PROCESSED_DIR / "lluvia_diaria_clean.parquet",
        exclusiones_lluvia=INTERMEDIATE_DIR / "exclusiones_lluvia.parquet",
        lluvia_diaria_observada=INTERMEDIATE_DIR / "lluvia_diaria_observada.parquet",
        report=REPORTS_DIR / "phase3_rainfall_audit.md",
    )

    clean.to_parquet(artifacts.lluvia_diaria_clean, index=False)
    exclusions.to_parquet(artifacts.exclusiones_lluvia, index=False)
    observed.to_parquet(artifacts.lluvia_diaria_observada, index=False)
    _write_rainfall_report(artifacts.report, summary=summary, exclusions=exclusions, source_metadata=source_metadata)

    return {
        "summary": summary,
        "artifacts": {field: str(getattr(artifacts, field)) for field in artifacts.__dataclass_fields__},
        "exclusion_counts": exclusions["motivo_exclusion"].value_counts().sort_index().to_dict(),
        "source_metadata": source_metadata,
    }


if __name__ == "__main__":
    result = run_rainfall_pipeline()
    print(pd.Series(result["summary"]).to_string())
