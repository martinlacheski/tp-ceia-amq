# Notas de features de lluvia

## Exploración de `observacion_codigo`

- Se normalizó `observacion_codigo` a cuatro categorías simples: `plain_a`, `missing`, `marca_horaria`, `evento_intenso`, más `otro_codigo` como residual.
- Se acepta `obs_evento_intenso_flag` porque la categoría `evento_intenso` concentra `462` días, con lluvia media `13.14 mm` y `287` días lluviosos, claramente por encima de `plain_a`/`missing`.
- Se rechazan one-hot por token individual (`B`, `R`, `•`, `°`, etc.) porque son más opacos y dispersos para esta etapa base.

## Resumen por categoría

| categoria | dias | dias_con_lluvia | lluvia_media_mm | lluvia_mediana_mm |
|---|---:|---:|---:|---:|
| `plain_a` | 856 | 145 | 3.59 | 0.00 |
| `evento_intenso` | 462 | 287 | 13.14 | 3.35 |
| `missing` | 253 | 56 | 3.21 | 0.00 |
| `marca_horaria` | 219 | 39 | 1.69 | 0.00 |
| `otro_codigo` | 126 | 48 | 7.05 | 0.00 |
