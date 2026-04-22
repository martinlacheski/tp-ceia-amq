# Auditoría de depuración — Phase 2

## Regla temporal canónica de `tramites`

- Fuente temporal canónica: `dt_inicio`.
- Regla explícita: conservar solo reclamos con `dt_inicio` entre `2021-01-01 00:00:00` y `2026-03-31 23:59:59` inclusive.
- Auditoría temporal: `16` filas difieren contra `fechainicio` + `horainicio`; se conserva `dt_inicio` por consistencia y cobertura completa.

## Regla geográfica

- Bounds válidos: latitud `-30.0` a `-25.0`, longitud `-57.0` a `-53.0`.
- Se detectaron `718` reclamos con columnas `geo1`/`geo2` invertidas y se corrigieron antes de persistir `lat`/`lon`.

## Resultados

- Reclamos totales analizados: `372551`.
- Reclamos retenidos en `data/processed/reclamos_clean.parquet`: `159030`.
- Reclamos excluidos en `data/intermediate/exclusiones_reclamos.parquet`: `213521`.
- Tareas retenidas en `data/processed/tareas_clean.parquet`: `254`.
- Tareas excluidas en `data/intermediate/exclusiones_tareas.parquet`: `325`.
- Destinos únicos para ruteo en `data/processed/destinos_unicos.parquet`: `14774`.

## Exclusiones de reclamos por motivo

- `duplicate_reclamo_id`: `14`
- `fecha_fuera_rango_lluvias`: `188879`
- `geo_missing_or_non_numeric`: `24626`
- `geo_zero_placeholder`: `2`

## Exclusiones de tareas por motivo

- `tarea_huerfana_sin_reclamo_clean`: `325`

## Mapeo reclamo ↔ tarea

- Clave de enlace retenida: `reclamo_id = NumeroTramite-NumeroOrden`.
- En tareas se mapeó `codigotar -> tarea_id`, `fecha/hora -> fecha_tarea` y `tarea -> tipo_tarea`.
- Solo se retienen tareas cuyo `reclamo_id` existe en `reclamos_clean`.

## Outputs generados

- `data/intermediate/exclusiones_reclamos.parquet`
- `data/intermediate/exclusiones_tareas.parquet`
- `data/intermediate/tramites_temporal_audit.parquet`
- `data/processed/reclamos_clean.parquet`
- `data/processed/tareas_clean.parquet`
- `data/processed/destinos_unicos.parquet`
