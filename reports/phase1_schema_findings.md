# Hallazgos básicos de esquema — inputs CEML

## `tramites.parquet`

- Filas observadas: **372551**.
- Columnas reales: `NumeroTramite`, `NumeroOrden`, `codigotra`, `descrtra`, `soccod`, `usucod`, `socnom`, `poscod`, `posloc`, `dircod`, `dirdes`, `usudir`, `geo1`, `geo2`, `sercod`, `serdes`, `codigotipo`, `descrtipo`, `codigomotivo`, `descrmotivo`, `celcod`, `celdes`, `viacod`, `viades`, `fechainicio`, `horainicio`, `fechafin`, `horafin`, `observaciones`, `observaciones1`, `observaciones2`, `observaciones3`, `observaciones4`, `finalizado`, `anulado`, `cumplido`, `usunick`, `codigoarea_inicio`, `area_inicio`, `codigoarea_destino`, `area_destino`, `dt_inicio`, `dt_fin`.
- Hallazgos útiles:
  - Existen **dos representaciones temporales**: `fechainicio/horainicio` + `fechafin/horafin` y también `dt_inicio/dt_fin`.
  - `geo1` y `geo2` llegan como **texto**, no como numérico; ambos tienen **68565 nulos**.
  - `dt_fin`, `fechafin` y `horafin` tienen **44315 nulos**.
  - `descrmotivo` tiene **132586 nulos**; el motivo `SIN ENERGIA` existe y aparece como categoría relevante para el filtrado posterior.
  - `serdes` viene completo y permite separar `Energia` del resto de los servicios.

## `tramites_tareas.parquet`

- Filas observadas: **579**.
- Columnas reales: `NumeroTramite`, `NumeroOrden`, `fecha`, `hora`, `codigotar`, `tarea`.
- Hallazgos útiles:
  - No trae `tarea_id`, `fecha_tarea` ni `tipo_tarea` con esos nombres; habrá que mapearlos explícitamente en Fase 2.
  - No se observaron nulos en las columnas actuales.
  - La granularidad parece estar ligada a `NumeroTramite` + `NumeroOrden`.

## Implicancias para la siguiente fase

1. La limpieza de reclamos debe fijar una única fuente temporal canónica antes de generar `reclamos_clean`.
2. La geolocalización requiere coerción a float y validación de bounds antes de cualquier exclusión.
3. El contrato mínimo de tareas deberá resolverse mediante renombre (`codigotar -> tarea_id`, `fecha -> fecha_tarea`, `tarea -> tipo_tarea`) y probablemente conservar `NumeroOrden` como clave auxiliar.
