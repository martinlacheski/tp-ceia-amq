# Referencias operativas — preparación Phase 3

## Sedes operativas

- `sede_id`: `agua_cloacas` | `sede_nombre`: `Sede Agua Potable y Cloacas` | coordenadas `-26.582934060558625`, `-54.73012469788584`
- `sede_id`: `energia` | `sede_nombre`: `Sede Energía Eléctrica y Alumbrado Público` | coordenadas `-26.56546259837734`, `-54.75411204580389`
- `sede_id`: `tv_internet` | `sede_nombre`: `Sede Televisión e Internet` | coordenadas `-26.568394449145398`, `-54.754005706687934`

## Mapeo observado servicio → sede

- `Agua Piray` → `agua_cloacas` (`Sede Agua Potable y Cloacas`) vía `explicit_override`
- `Agua Potable` → `agua_cloacas` (`Sede Agua Potable y Cloacas`) vía `sedes_servicios.json`
- `Cloaca` → `agua_cloacas` (`Sede Agua Potable y Cloacas`) vía `explicit_override`
- `Adicionales` → `energia` (`Sede Energía Eléctrica y Alumbrado Público`) vía `default_energia_fallback`
- `Alumbrado Publico` → `energia` (`Sede Energía Eléctrica y Alumbrado Público`) vía `sedes_servicios.json`
- `Ambulancia` → `energia` (`Sede Energía Eléctrica y Alumbrado Público`) vía `default_energia_fallback`
- `Bomberos` → `energia` (`Sede Energía Eléctrica y Alumbrado Público`) vía `default_energia_fallback`
- `Energia` → `energia` (`Sede Energía Eléctrica y Alumbrado Público`) vía `sedes_servicios.json`
- `Energia Prepaga` → `energia` (`Sede Energía Eléctrica y Alumbrado Público`) vía `sedes_servicios.json`
- `Facturas Adicionales` → `energia` (`Sede Energía Eléctrica y Alumbrado Público`) vía `default_energia_fallback`
- `Hogar Niños` → `energia` (`Sede Energía Eléctrica y Alumbrado Público`) vía `default_energia_fallback`
- `Hogares` → `energia` (`Sede Energía Eléctrica y Alumbrado Público`) vía `default_energia_fallback`
- `Hospital` → `energia` (`Sede Energía Eléctrica y Alumbrado Público`) vía `default_energia_fallback`
- `Seg. Sepelio` → `energia` (`Sede Energía Eléctrica y Alumbrado Público`) vía `default_energia_fallback`
- `Seguro de Hogar` → `energia` (`Sede Energía Eléctrica y Alumbrado Público`) vía `default_energia_fallback`
- `Tablero Movil` → `energia` (`Sede Energía Eléctrica y Alumbrado Público`) vía `default_energia_fallback`
- `Transmision de Datos` → `tv_internet` (`Sede Televisión e Internet`) vía `sedes_servicios.json`
- `TV Aire` → `tv_internet` (`Sede Televisión e Internet`) vía `sedes_servicios.json`
- `TV Cable` → `tv_internet` (`Sede Televisión e Internet`) vía `sedes_servicios.json`

## Costos base

- `vehiculo_tipo`: `vehiculo_promedio_flota`
- `costo_km`: `305.73` ARS/km
- `costo_hora`: `33187.0` ARS/hora
- `combustible_tipo`: `Diesel`
- `combustible_precio_litro_ars`: `2461.0`
- `rendimiento_km_litro`: `9.0`

## Outputs generados

- `data/processed/sede_ref.parquet`
- `data/processed/servicio_sede_ref.parquet`
- `data/processed/costos_ref.parquet`
