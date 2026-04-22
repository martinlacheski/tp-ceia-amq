# Baseline heurístico `zona-diario` (t+1)

## Enfoque

- **Tiempo futuro**: mediana histórica por `zona_id` y mismo mes; si falta historia suficiente, cae a estación trimestral, mediana trailing 28 días, historia de zona y finalmente mediana global.
- **Reclamos futuros**: heurística estacional simple `semana_anterior -> mediana trailing 28d -> historia de zona -> global`.
- **Costo futuro**: `baseline_reclamos * costo_por_reclamo histórico`, usando como prioridad mes de la zona, trailing 28 días, historia de zona y fallback global.
- Todo el baseline es **online/past-only**: cada predicción usa únicamente observaciones disponibles antes del corte de esa fila.

## Métricas comparables

| target | ventana | filas | MAE | RMSE | WMAPE | total_real | total_predicho |
|---|---|---:|---:|---:|---:|---:|---:|
| `tiempo` | `full_history` | 75451 | 105.63 | 278.25 | 0.6956 | 11456676.75 | 8845111.15 |
| `tiempo` | `recent_180d` | 6731 | 117.02 | 300.59 | 0.6821 | 1154700.75 | 880550.40 |
| `costo` | `full_history` | 75451 | 61347.27 | 159176.38 | 0.7840 | 5903715973.68 | 5903648424.46 |
| `costo` | `recent_180d` | 6731 | 67348.98 | 162358.78 | 0.7681 | 590157322.78 | 587854489.13 |
| `reclamos` | `full_history` | 75451 | 1.56 | 4.19 | 0.7406 | 158957.00 | 159212.00 |
| `reclamos` | `recent_180d` | 6731 | 1.75 | 4.58 | 0.7306 | 16123.00 | 16130.00 |

## Cobertura por regla heurística

| target | regla | filas |
|---|---|---:|
| `tiempo` | `zona_mes_mediana` | 72901 |
| `tiempo` | `zona_estacion_mediana` | 1695 |
| `tiempo` | `zona_trailing_28d` | 640 |
| `tiempo` | `historia_global` | 210 |
| `tiempo` | `global_fallback` | 5 |
| `costo` | `reclamos_baseline_x_costo_mes` | 50807 |
| `costo` | `reclamos_baseline_x_costo_zona` | 16494 |
| `costo` | `reclamos_baseline_x_costo_global` | 6221 |
| `costo` | `reclamos_baseline_x_costo_trailing_28d` | 1924 |
| `costo` | `global_fallback` | 5 |
| `reclamos` | `semana_anterior` | 75150 |
| `reclamos` | `historia_global` | 294 |
| `reclamos` | `global_fallback` | 7 |

## Lectura rápida

- Este benchmark ya deja una vara explícita para las tandas ML: cualquier modelo futuro tiene que mejorar estas métricas manteniendo el esquema temporal sin leakage.
- La señal principal sigue siendo **tiempo operativo futuro**; costo acompaña como traducción monetaria y reclamos como presión futura de demanda.
