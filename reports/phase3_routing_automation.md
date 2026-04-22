# Automatización de ruteo Google Maps — Phase 3.3

## Presupuesto explícito

- `batch_size`: `250` destinos.
- `max_batches`: `4`.
- `max_runtime_minutes`: `20`.
- `batch_sleep_seconds`: `0`.
- `run_started_at`: `2026-04-19T12:17:17.527496+00:00`.
- `run_finished_at`: `2026-04-19T12:17:34.284684+00:00`.
- `elapsed_minutes`: `0.28`.
- `stop_reason`: `all_pending_resolved`.

## Estado final del cache

- Destinos únicos: `28524`.
- Destinos `ok`: `28524`.
- Destinos `zero_results`: `0`.
- Destinos `api_error`: `0`.
- Destinos `request_error`: `0`.
- Destinos `pending`: `0`.

## Historial por batch

- Batch 1: attempted=0, ok_total=28524, pending_total=0, status=stopped, stop_reason=all_pending_resolved.

## Persistencia

- Cada batch reescribe `data/intermediate/distancias_cache.parquet` y `data/intermediate/routing_scope.parquet`.
- Cada batch refresca `data/processed/distancias_costos.parquet` y `data/processed/reclamos_enriquecidos.parquet`.
- El historial acumulado queda en `data/intermediate/routing_batch_history.parquet`.
