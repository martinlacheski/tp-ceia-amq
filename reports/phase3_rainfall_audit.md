# Auditoría de lluvia diaria — Phase 3

## Regla de normalización

- Ventana retenida: `2021-01-01` → `2026-03-31` inclusive.
- La fecha final se construye desde `año_extraido` + `mes_extraido` + `dia`.
- Se excluyen meses inválidos, fechas imposibles, lluvia no numérica/negativa y filas fuera de rango.
- `lluvia_diaria_clean.parquet` queda alineado a calendario completo; los huecos se marcan `missing_source` sin inventar `lluvia_mm`.

## Resultados

- Filas crudas analizadas: `2012`.
- Filas excluidas a auditoría: `96`.
- Días observados dentro del rango: `1916`.
- Días calendario persistidos: `1916`.
- Días sin observación (`missing_source`): `0`.
- Días con duplicados colapsados: `0`.

## Fuente reproducible

- Input OCR base: `/home/martin/Code/tp-ceia-amq/data/raw/rain/dataset_lluvias_diario_con_obs.csv`.
- Input manual de recuperación: `/home/martin/Code/tp-ceia-amq/data/raw/rain/rainfall_manual_recovered_months.csv`.
- Filas manuales cargadas: `182`.
- Filas efectivamente aplicadas sobre el input base: `152`.
- Documentos recuperados manualmente: `11. Registro de lluvia 11-25.pdf, 8. Registro de lluvia 08-2025.pdf, 9. Registro de lluvia 09-2025.pdf, Registro lluvias 08-2022.pdf, Registro lluvias 09-2022.pdf, registro 04-2022.jpg`.

## Exclusiones por motivo

- `fecha_fuera_rango_modelado`: `62`
- `fecha_invalida`: `33`
- `mes_invalido`: `1`

## Outputs generados

- `data/raw/rain/rainfall_manual_recovered_months.csv`
- `data/processed/lluvia_diaria_clean.parquet`
- `data/intermediate/exclusiones_lluvia.parquet`
- `data/intermediate/lluvia_diaria_observada.parquet`
