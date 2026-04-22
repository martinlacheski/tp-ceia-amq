# Costeo operativo inicial — Phase 3.3

## Objetivo

- Expandir el cache de rutas en tandas controladas y traducir la cobertura actual a un dataset de costo operativo por reclamo.
- Mantener `reclamos_enriquecidos.parquet` como vista completa del universo limpio, dejando `distancias_costos.parquet` solo con reclamos costeables (`routing_status = ok`).

## Resultados

- Reclamos limpios totales: `159030`.
- Reclamos enriquecidos totales: `159030`.
- Reclamos con costo operativo disponible: `159030`.
- Destinos `ok` actualmente en cache: `28524`.
- Destinos `pending` actualmente en cache: `0`.
- Reclamos aún no costeables por falta de ruta OK: `0`.
- Costo operativo total cubierto por la tanda actual: `1211976276.77` ARS.
- Sedes operativas cubiertas: `3`.

## Estados de ruteo en `reclamos_enriquecidos.parquet`

- `ok`: `159030` reclamos

## Cobertura por sede

- `energia` (Sede Energía Eléctrica y Alumbrado Público): reclamos=`92635`, ok=`92635`, servicios=`13`, costo_total=`787784446.29` ARS
- `tv_internet` (Sede Televisión e Internet): reclamos=`43937`, ok=`43937`, servicios=`3`, costo_total=`269677357.87` ARS
- `agua_cloacas` (Sede Agua Potable y Cloacas): reclamos=`22458`, ok=`22458`, servicios=`3`, costo_total=`154514472.60` ARS

## Cobertura por servicio

- `Agua Potable` → `agua_cloacas`: reclamos=`20195`, ok=`20195`, costo_total=`132426167.35` ARS
- `Cloaca` → `agua_cloacas`: reclamos=`1142`, ok=`1142`, costo_total=`7291655.44` ARS
- `Agua Piray` → `agua_cloacas`: reclamos=`1121`, ok=`1121`, costo_total=`14796649.81` ARS
- `Energia` → `energia`: reclamos=`66703`, ok=`66703`, costo_total=`589389058.00` ARS
- `Alumbrado Publico` → `energia`: reclamos=`11180`, ok=`11180`, costo_total=`84396190.44` ARS
- `Seg. Sepelio` → `energia`: reclamos=`2997`, ok=`2997`, costo_total=`26731531.22` ARS
- `Adicionales` → `energia`: reclamos=`2843`, ok=`2843`, costo_total=`22827795.59` ARS
- `Facturas Adicionales` → `energia`: reclamos=`2833`, ok=`2833`, costo_total=`22796855.63` ARS
- `Seguro de Hogar` → `energia`: reclamos=`2136`, ok=`2136`, costo_total=`18963594.62` ARS
- `Energia Prepaga` → `energia`: reclamos=`1946`, ok=`1946`, costo_total=`12104858.42` ARS
- `Ambulancia` → `energia`: reclamos=`964`, ok=`964`, costo_total=`4396666.32` ARS
- `Bomberos` → `energia`: reclamos=`410`, ok=`410`, costo_total=`2285190.88` ARS
- `Tablero Movil` → `energia`: reclamos=`328`, ok=`328`, costo_total=`2470840.35` ARS
- `Hogar Niños` → `energia`: reclamos=`252`, ok=`252`, costo_total=`1224946.50` ARS
- `Hospital` → `energia`: reclamos=`41`, ok=`41`, costo_total=`193763.15` ARS
- `Hogares` → `energia`: reclamos=`2`, ok=`2`, costo_total=`3155.17` ARS
- `TV Cable` → `tv_internet`: reclamos=`22347`, ok=`22347`, costo_total=`122039145.30` ARS
- `Transmision de Datos` → `tv_internet`: reclamos=`21481`, ok=`21481`, costo_total=`147315917.29` ARS
- `TV Aire` → `tv_internet`: reclamos=`109`, ok=`109`, costo_total=`322295.28` ARS

## Outputs generados

- `data/processed/distancias_costos.parquet`
- `data/processed/reclamos_enriquecidos.parquet`
