from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
RAW_CEML_DIR = RAW_DIR / "ceml"
RAW_ARCHIVOS_DIR = RAW_DIR / "archivos"
RAW_RAIN_DIR = RAW_DIR / "rain"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
PROCESSED_DIR = DATA_DIR / "processed"
REPORTS_DIR = ROOT / "reports"
NOTEBOOKS_DIR = ROOT / "notebooks"
HELPERS_DIR = ROOT / "helpers"
ENV_FILE = ROOT / ".env"

MANDATORY_INPUTS = {
    "tramites": RAW_CEML_DIR / "tramites.parquet",
    "tramites_tareas": RAW_CEML_DIR / "tramites_tareas.parquet",
    "costos_vehiculos": RAW_ARCHIVOS_DIR / "costos_vehiculos.json",
    "sedes_servicios": RAW_ARCHIVOS_DIR / "sedes_servicios.json",
    "tarifas_ceml": RAW_ARCHIVOS_DIR / "tarifas_ceml_2026_03.json",
    "tasas_pdf": RAW_ARCHIVOS_DIR / "Tasas 03-2026.pdf",
    "lluvias": RAW_RAIN_DIR / "dataset_lluvias_diario_con_obs.csv",
}


def ensure_workspace_layout() -> None:
    for path in (
        NOTEBOOKS_DIR,
        HELPERS_DIR,
        RAW_CEML_DIR,
        RAW_ARCHIVOS_DIR,
        RAW_RAIN_DIR,
        INTERMEDIATE_DIR,
        PROCESSED_DIR,
        REPORTS_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)


def load_env(env_file: Path = ENV_FILE) -> dict[str, str]:
    values: dict[str, str] = {}
    if not env_file.exists():
        return values

    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def get_env_var(name: str, default: str | None = None) -> str | None:
    return load_env().get(name, default)


def require_input(name: str) -> Path:
    try:
        path = MANDATORY_INPUTS[name]
    except KeyError as exc:
        raise KeyError(f"Input desconocido: {name}") from exc

    if not path.exists():
        raise FileNotFoundError(f"No existe el input obligatorio '{name}' en {path}")
    return path
