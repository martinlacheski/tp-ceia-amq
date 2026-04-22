# Decisión de clustering espacial para `zona_id`

## Criterio implementado

- Método práctico: clustering greedy con restricción explícita de diámetro máximo `<= 5 km` entre cualquier nuevo punto y los miembros ya asignados del cluster.
- Diámetro objetivo solicitado: `5 km`.
- Justificación: no dependemos de `scikit-learn`, evitamos el efecto cadena de DBSCAN/simple-linkage y mantenemos una partición más defendible para el grano `zona-diario`.
- Advertencia metodológica: la asignación es greedy (ordenada por volumen de reclamos), así que no garantiza el óptimo global; sí garantiza control práctico del diámetro observado y trazabilidad reproducible.

## Resumen

- Zonas generadas: `43`.
- Destinos únicos clusterizados: `14772`.
- Reclamos cubiertos por el mapeo: `159030`.
- `eps` efectivo: `2.5` km.
- Clusters que exceden el diámetro objetivo: `0`.
- Diámetro máximo observado: `4.989` km.
- Mediana de diámetro observado: `4.501` km.

## Zonas con más reclamos

| zona_id | reclamos | destinos | diámetro_km | excede_5km |
|---|---:|---:|---:|---|
| `zona_0001` | 38748 | 2897 | 4.902 | no |
| `zona_0002` | 29951 | 2459 | 3.691 | no |
| `zona_0003` | 20435 | 1529 | 4.979 | no |
| `zona_0004` | 15227 | 2486 | 4.367 | no |
| `zona_0005` | 14174 | 1189 | 4.308 | no |
| `zona_0006` | 5676 | 595 | 4.764 | no |
| `zona_0007` | 5387 | 453 | 4.962 | no |
| `zona_0008` | 4353 | 559 | 3.611 | no |
| `zona_0009` | 3547 | 288 | 4.627 | no |
| `zona_0010` | 3381 | 344 | 4.401 | no |
