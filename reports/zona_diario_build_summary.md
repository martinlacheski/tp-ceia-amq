# Resumen de construcción `zona-diario`

- Filas `zona_diario_base`: `75494`.
- Filas `zona_diario_supervisado`: `75494`.
- Zonas únicas: `43`.
- Rango temporal base: `2021-01-01` → `2026-03-31`.
- Días con reclamos: `25791`.
- Días sin reclamos pero retenidos para historial/targets: `49703`.

## Features iniciales incluidas

- Traslado: `traslado_min_total`, `traslado_min_promedio`, `distance_km_total`.
- Resolución heurística: `resolucion_base_min_total`, `tiempo_total_operativo_min`.
- Costo soporte: componentes laborales, km, combustible y `costo_total_compuesto_ars`.
- Lluvia: `llovio`, `lluvia_mm`, `lluvia_intensidad`, `lluvia_status`, `obs_evento_intenso_flag`.
- Supervisión futura: `y_tiempo_t+1_min`, `y_costo_t+1_ars`, `y_reclamos_t+1`.
