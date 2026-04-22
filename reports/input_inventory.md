# Inventario de inputs mínimos — Phase 1

## Workspace canónico

Todo el workflow del cambio `ceml-inversion-notebook` queda bajo `ceia-amq-tp/`.

## Inputs obligatorios ubicados

| Dataset | Origen | Destino canónico | Estado |
|---|---|---|---|
| `tramites.parquet` | `TP/CEML/data/tramites.parquet` | `data/raw/ceml/tramites.parquet` | copiado |
| `tramites_tareas.parquet` | `TP/CEML/data/tramites_tareas.parquet` | `data/raw/ceml/tramites_tareas.parquet` | copiado |
| `costos_vehiculos.json` | `TP/Archivos/costos_vehiculos.json` | `data/raw/archivos/costos_vehiculos.json` | copiado |
| `sedes_servicios.json` | `TP/Archivos/sedes_servicios.json` | `data/raw/archivos/sedes_servicios.json` | copiado |
| `tarifas_ceml_2026_03.json` | `TP/Archivos/tarifas_ceml_2026_03.json` | `data/raw/archivos/tarifas_ceml_2026_03.json` | copiado |
| `Tasas 03-2026.pdf` | `TP/Archivos/Tasas 03-2026.pdf` | `data/raw/archivos/Tasas 03-2026.pdf` | copiado |
| `dataset_lluvias_diario_con_obs.csv` | `data/raw/rain/dataset_lluvias_diario_con_obs.csv` | `data/raw/rain/dataset_lluvias_diario_con_obs.csv` | ya presente |

## Decisiones de layout aplicadas

- `helpers/` vive en la raíz del workspace y reemplaza cualquier helper dentro de `src/ceml_inversion/`.
- `notebooks/` vive en la raíz del workspace.
- `data/raw/`, `data/intermediate/` y `data/processed/` quedan alineados al flujo definitivo.
- No se usarán artefactos nuevos en `TP/CEML/outputs/`.
