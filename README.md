# Predicción de demanda operativa y priorización de inversión en infraestructura mediante aprendizaje automático en la Cooperativa de Electricidad de Montecarlo Limitada (CEML)

Proyecto de análisis y preparación de datos para el TP de la materia Aprendizaje de Máquina de la Carrera Especialización en Inteligencia Artificial de la Universidad de Buenos Aires, sobre reclamos operativos de la CEML. El workspace concentra la limpieza de fuentes crudas, la construcción de datasets analíticos por zona y fecha, la exploración de variables, y una app interactiva para navegar los datos depurados antes de pasar al entrenamiento de modelos.

### **Autor:** Martín Anibal Lacheski  

## Qué incluye el repositorio

- **Pipeline de preparación** en `helpers/` para limpiar reclamos, integrar lluvia, costeo y construir la base `zona-diario`.
- **Artifacts procesados** en `data/processed/` e `data/intermediate/` listos para exploración, baseline y modelado posterior.
- **Notebooks de trabajo** para ingesta, depuración, lluvia, costeo/ruteo, exploración y notebook final de entrega.
- **App Streamlit mínima** para navegar datasets depurados con filtros interactivos.

## Estructura principal

```
ceia-amq-tp/
  helpers/
  notebooks/
  streamlit_app/
  data/
    raw/
      ceml/
      archivos/
      rain/
    intermediate/
    processed/
  reports/
  .venv/
  .env
```

## Componentes clave

- `notebooks/1_ingesta_depuration.ipynb` — limpieza inicial de trámites y tareas.
- `notebooks/2_lluvia_depuration.ipynb` — depuración y normalización de lluvia.
- `notebooks/3_costeo_ruteo.ipynb` — integración operativa y costeo.
- `notebooks/4_exploracion.ipynb` — exploración de datos, calidad y proceso de preparación de features.
- `notebooks/5_tp_final_amq.ipynb` — notebook final principal de entrega.
- `streamlit_app/app.py` — navegador interactivo de datasets depurados.
- `helpers/eda_utils.py` — utilidades compartidas para inventario, carga y perfilado.

## Requisitos

- Python 3.11 o superior
- Datos presentes dentro de `data/raw/`, `data/intermediate/` y `data/processed/`

## Datos del trabajo y alcance de uso

Este repositorio contiene el material de trabajo del TP de CEIA/AMQ para el caso CEML, incluyendo:

- notebooks metodológicas y notebook final,
- helpers de preparación y exploración,
- reports de auditoría y trazabilidad,
- artifacts de datos en `data/raw/`, `data/intermediate/` y `data/processed/`.

### Importante sobre los datos

Los **datos no forman parte de la licencia open source del código**. Los registros operativos, documentos de lluvia, referencias de costeo y demás información provista por la **Cooperativa Eléctrica de Montecarlo Limitada (CEML)** se incluyen únicamente con fines **académicos y de evaluación** del trabajo práctico.

Por lo tanto:

- el **código fuente** del repositorio se distribuye bajo licencia **MIT**,
- los **datos y documentos de CEML** mantienen carácter **restringido / propietario**,
- no se autoriza su redistribución, reutilización ni explotación fuera de este contexto sin autorización expresa de la cooperativa.

Si vas a publicar este proyecto en GitHub, conviene revisar especialmente qué contenidos de `data/raw/`, `data/intermediate/` y `data/processed/` querés exponer públicamente.

## Crear y usar el entorno virtual

La forma recomendada de trabajar este repo es usando el `pyproject.toml` del proyecto como fuente de dependencias.

### Opción recomendada: usando `uv`

Desde la carpeta `ceia-amq-tp/`:

```bash
uv venv .venv
source .venv/bin/activate
uv sync
```

Qué hace cada paso:

- `uv venv .venv` crea el entorno virtual dentro del proyecto.
- `source .venv/bin/activate` activa el entorno.
- `uv sync` instala las dependencias declaradas en `pyproject.toml` y respeta `uv.lock` si está presente.

### Alternativa si ya existe `.venv`

```bash
source .venv/bin/activate
uv sync
```

### Archivo `.env` local

Si necesitás variables de entorno locales (por ejemplo `GCP_API_KEY`), creá un archivo `.env` en la raíz del repo a partir de `env_example`.

```bash
cp env_example .env
```

> `.env` es solo para uso local y no debe versionarse en GitHub.

### Verificación rápida

Con el entorno activado, podés validar que quedó bien así:

```bash
python -c "import pandas, pyarrow, matplotlib, seaborn, streamlit, jupyterlab, ipykernel; print('entorno ok')"
```

> Nota: en esta documentación dejamos como flujo principal el basado en `pyproject.toml`. No hace falta `requirements.txt` mientras uses `uv sync`.
> Este repo se usa como workspace ejecutable (notebooks + Streamlit), no como paquete distribuible; por eso `uv` está configurado para sincronizar dependencias sin intentar empaquetar el proyecto.

## Cómo ejecutar el proyecto

### 1. Abrir notebooks en JupyterLab

Con el entorno activado:

```bash
uv run jupyter lab
```

Después abrí alguna de estas notebooks desde la interfaz de JupyterLab:

- `notebooks/1_ingesta_depuration.ipynb`
- `notebooks/2_lluvia_depuration.ipynb`
- `notebooks/3_costeo_ruteo.ipynb`
- `notebooks/4_exploracion.ipynb`
- `notebooks/5_tp_final_amq.ipynb`

#### Nota sobre mapas interactivos

Los mapas Folium embebidos se visualizan mejor en **JupyterLab**. Si abrís la notebook final en VS Code, puede aparecer el mensaje de confianza para cargar mapas interactivos.

Lanzar JupyterLab desde la raíz del proyecto ayuda a que las rutas relativas de datos y artifacts funcionen sin ajustes adicionales.

### 2. Levantar la app interactiva

Con el entorno activado:

```bash
streamlit run streamlit_app/app.py
```

La app consume los parquets de `data/processed/` y permite filtrar por:

- rango de fechas
- zona
- servicio
- condición de lluvia

Además muestra métricas resumen, series temporales, tabla filtrada y perfil básico de variables.

## Flujo sugerido de uso

1. Crear o activar `.venv` y ejecutar `uv sync`.
2. Verificar que los artifacts procesados estén presentes en `data/processed/`.
3. Ejecutar `notebooks/4_exploracion.ipynb` para revisar calidad y preparación de datos.
4. Usar `streamlit_app/app.py` para navegar interactivamente los datos depurados.
5. Continuar con `notebooks/5_tp_final_amq.ipynb` para baseline, modelado, conclusiones y entrega final.

## Comandos rápidos

Desde `ceia-amq-tp/`:

```bash
uv venv .venv
source .venv/bin/activate
uv sync
uv run jupyter lab
```

O para abrir directamente la app:

```bash
uv venv .venv
source .venv/bin/activate
uv sync
streamlit run streamlit_app/app.py
```

## Referencias útiles

- `helpers/paths.py`
- `helpers/contracts.py`
- `helpers/eda_utils.py`
- `reports/input_inventory.md`
- `reports/phase2_cleaning_audit.md`
- `reports/zona_diario_build_summary.md`

## Licencia

El **código fuente** de este repositorio se distribuye bajo licencia **MIT**. Ver archivo [`LICENSE`](./LICENSE).

Esa licencia **no aplica a los datos** incluidos en el proyecto, que siguen el criterio de uso restringido descrito en la sección **Datos del trabajo y alcance de uso**.
