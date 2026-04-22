# Agregados operativos mínimos — Phase 3.4

## Objetivo

- Resumir el universo enriquecido por fecha para habilitar argumentos de recurrencia, costo y cobertura.
- Dejar un ranking de hotspots operativos reutilizable por el notebook final sin recalcular el costeo.

## Resultados

- Días resumidos: `1915`.
- Hotspots únicos: `29787`.
- Costo operativo total resumido: `1211976276.77` ARS.
- Reclamos ruteados acumulados: `159030`.

## Hotspot líder actual

- `sede_id`: `energia`.
- `sede_nombre`: `Sede Energía Eléctrica y Alumbrado Público`.
- `destino_key`: `-26.57037_-54.57839`.
- `localidad_referencia`: `MONTECARLO`.
- `direccion_referencia`: `LAHARRAGUE`.
- `reclamos_total`: `234`.
- `reclamos_ruteados_ok`: `234`.
- `costo_operativo_total_ars`: `4958098.47`.

## Outputs generados

- `data/processed/resumen_operativo_diario.parquet`
- `data/processed/resumen_hotspots.parquet`
