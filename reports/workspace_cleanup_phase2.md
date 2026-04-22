# Limpieza conservadora de workspace — antes de Phase 2

## Criterio aplicado

- Se removió **solo** lo inequívocamente obsoleto/incompatible con la estructura final del workspace.
- No se tocó `.env` ni tooling necesario para ejecutar notebooks/helpers.
- No se borraron datasets extra que podrían seguir siendo útiles más adelante si no era 100% inequívoco que sobraban.

## Eliminaciones realizadas

| Ruta | Motivo |
|---|---|
| `helpers/__pycache__/` | Artefacto compilado local de Python, no fuente ni insumo del workflow final. |

## Resultado

- El workspace final sigue concentrado en `helpers/`, `notebooks/`, `data/{raw,intermediate,processed}/`, `reports/` y `.env`.
- No se utiliza ni recrea `src/ceml_inversion/` ni `TP/CEML/outputs/` para artefactos nuevos.
