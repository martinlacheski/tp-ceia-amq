# Costeo operativo inicial — Phase 3.3

## Objetivo

- Expandir el cache de rutas en tandas controladas y traducir la cobertura actual a un dataset de costo operativo por reclamo.
- Mantener `reclamos_enriquecidos.parquet` como vista completa del universo limpio, dejando `distancias_costos.parquet` solo con reclamos costeables (`routing_status = ok`).

## Resultados

- Reclamos limpios totales: `159030`.
- Reclamos enriquecidos totales: `159030`.
- Reclamos con costo operativo disponible: `158978`.
- Destinos `ok` actualmente en cache: `28524`.
- Destinos `pending` actualmente en cache: `0`.
- Reclamos aún no costeables por falta de ruta OK: `52`.
- Costo operativo total cubierto por la tanda actual: `2964201104.10` ARS.
- Sedes operativas cubiertas: `3`.

## Estados de ruteo en `reclamos_enriquecidos.parquet`

- `ok`: `158978` reclamos
- `pending`: `52` reclamos

## Cobertura por sede

- `energia` (Sede Energía Eléctrica y Alumbrado Público): reclamos=`92635`, ok=`92583`, servicios=`13`, costo_total=`1926452023.08` ARS
- `tv_internet` (Sede Televisión e Internet): reclamos=`43937`, ok=`43937`, servicios=`3`, costo_total=`660068371.61` ARS
- `agua_cloacas` (Sede Agua Potable y Cloacas): reclamos=`22458`, ok=`22458`, servicios=`3`, costo_total=`377680709.41` ARS

## Cobertura por servicio

- `Agua Potable` → `agua_cloacas`: reclamos=`20195`, ok=`20195`, costo_total=`323586661.14` ARS
- `Cloaca` → `agua_cloacas`: reclamos=`1142`, ok=`1142`, costo_total=`17789113.91` ARS
- `Agua Piray` → `agua_cloacas`: reclamos=`1121`, ok=`1121`, costo_total=`36304934.36` ARS
- `Energia` → `energia`: reclamos=`66703`, ok=`66652`, costo_total=`1440691064.08` ARS
- `Alumbrado Publico` → `energia`: reclamos=`11180`, ok=`11180`, costo_total=`206652825.51` ARS
- `Seg. Sepelio` → `energia`: reclamos=`2997`, ok=`2997`, costo_total=`65482731.76` ARS
- `Adicionales` → `energia`: reclamos=`2843`, ok=`2843`, costo_total=`55912225.65` ARS
- `Facturas Adicionales` → `energia`: reclamos=`2833`, ok=`2833`, costo_total=`55836755.70` ARS
- `Seguro de Hogar` → `energia`: reclamos=`2136`, ok=`2135`, costo_total=`46392432.76` ARS
- `Energia Prepaga` → `energia`: reclamos=`1946`, ok=`1946`, costo_total=`29628422.97` ARS
- `Ambulancia` → `energia`: reclamos=`964`, ok=`964`, costo_total=`10739320.52` ARS
- `Bomberos` → `energia`: reclamos=`410`, ok=`410`, costo_total=`5589823.64` ARS
- `Tablero Movil` → `energia`: reclamos=`328`, ok=`328`, costo_total=`6050911.81` ARS
- `Hogar Niños` → `energia`: reclamos=`252`, ok=`252`, costo_total=`2994316.59` ARS
- `Hospital` → `energia`: reclamos=`41`, ok=`41`, costo_total=`473499.77` ARS
- `Hogares` → `energia`: reclamos=`2`, ok=`2`, costo_total=`7692.32` ARS
- `TV Cable` → `tv_internet`: reclamos=`22347`, ok=`22347`, costo_total=`298498525.79` ARS
- `Transmision de Datos` → `tv_internet`: reclamos=`21481`, ok=`21481`, costo_total=`360784058.91` ARS
- `TV Aire` → `tv_internet`: reclamos=`109`, ok=`109`, costo_total=`785786.91` ARS

## Outputs generados

- `data/processed/distancias_costos.parquet`
- `data/processed/reclamos_enriquecidos.parquet`
