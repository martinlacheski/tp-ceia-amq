# Preparación de ruteo Google Maps — Phase 3

## Supuestos activos

- El ruteo distingue cada origen por `(sede_id, destino_key)` para evitar sesgo de sede única.
- `agua_cloacas` se aplica a `Agua Potable`, `Agua Piray` y `Cloaca`.
- `tv_internet` se aplica a `Transmision de Datos`, `TV Cable` y `TV Aire`.
- `energia` conserva `Energia`, `Energia Prepaga`, `Alumbrado Publico` y los servicios no mapeados explícitamente.
- El scope se deriva de `reclamos_clean.parquet`, no de una sede fija.
- `max_new_requests` aplicado en esta corrida: `250`.

## Resultados

- Pares `(sede_id, destino_key)` evaluados: `28524`.
- Reclamos cubiertos por esos destinos: `159030`.
- Destinos `ok` en cache: `28524`.
- Destinos `zero_results`: `0`.
- Destinos con `api_error`: `0`.
- Destinos aún `pending`: `0`.
- Nuevas llamadas realizadas en esta corrida: `0`.
- Estado de API detectado: `configured`.

## Outputs generados

- `data/intermediate/distancias_cache.parquet`
- `data/intermediate/routing_scope.parquet`
- `data/processed/sede_ref.parquet`
- `data/processed/servicio_sede_ref.parquet`
- `data/processed/costos_ref.parquet`

## Cobertura por sede

- `energia`: pairs=`14118`, reclamos=`92635`, ok=`14118`, pending=`0`
- `agua_cloacas`: pairs=`7312`, reclamos=`22458`, ok=`7312`, pending=`0`
- `tv_internet`: pairs=`7094`, reclamos=`43937`, ok=`7094`, pending=`0`
