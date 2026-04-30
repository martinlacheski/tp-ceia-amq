# Verificación liviana de outputs `zona-diario`

- Duplicados `zona_id-fecha` en base: `0`.
- Reclamos sin `zona_id` luego del join: `0`.
- Targets nulos esperados (último día por zona): `43`.
- Filas entrenables con target disponible: `76360`.
- Check `lag1_uses_past_only`: `True`.
- Check `targets_have_future_shift`: `True`.

## Estado

- Validación estructural: OK.
- Anti-leakage básico por construcción `shift/rolling` sobre pasado: OK.
- Listo para baseline heurístico y entrenamiento posterior, sin ejecutar modelos todavía.
