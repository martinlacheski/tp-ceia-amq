# Verificación liviana de outputs `zona-diario`

- Duplicados `zona_id-fecha` en base: `0`.
- Reclamos sin `zona_id` luego del join: `0`.
- Targets nulos esperados (último día por zona): `43`.
- Filas entrenables con target disponible: `75451`.
- Check `lag1_uses_past_only`: `True`.
- Check `targets_have_future_shift`: `True`.

## Estado

- Validación estructural: OK.
- Anti-leakage básico por construcción `shift/rolling` sobre pasado: OK.
- Baseline heurístico persistido fuera de la notebook: OK.
- La notebook final sigue consumiendo artifacts procesados, sin recalcular el pipeline pesado: OK.

## Baseline heurístico — ventana reciente 180 días

| target | filas | MAE | RMSE | WMAPE |
|---|---:|---:|---:|---:|
| `tiempo` | 6731 | 117.02 | 300.59 | 0.6821 |
| `costo` | 6731 | 67348.98 | 162358.78 | 0.7681 |
| `reclamos` | 6731 | 1.75 | 4.58 | 0.7306 |
