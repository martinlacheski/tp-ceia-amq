from __future__ import annotations

import sys
from pathlib import Path

import folium
import pandas as pd
import streamlit as st
from folium.plugins import Fullscreen, MarkerCluster
from streamlit_folium import st_folium

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from helpers.eda_utils import (  # noqa: E402
    build_dataset_inventory,
    categorical_profile,
    dataset_overview,
    detect_date_column,
    filter_dataframe_by_services,
    filter_allowed_services,
    get_allowed_services,
    load_dataset,
    normalize_service_label,
    numeric_profile,
    prepare_claim_map_dataset,
    prepare_zone_map_dataset,
    quality_profile,
    resolve_service_dimension,
)


st.set_page_config(page_title="CEIA AMQ TP · Exploración", layout="wide")

DATASET_LABELS = {
    "zona_diario_supervisado": "Zona diario supervisado",
    "zona_diario_base": "Zona diario base",
    "reclamos_zonificados": "Reclamos zonificados",
}

DEFAULT_METRICS = [
    "reclamos_count",
    "tiempo_total_operativo_min",
    "costo_total_compuesto_ars",
    "lluvia_mm",
    "y_reclamos_t+1",
]


@st.cache_data(show_spinner=False)
def load_app_dataset(dataset_name: str) -> pd.DataFrame:
    return load_dataset(dataset_name)


def _format_number(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{value:,.0f}".replace(",", ".")


def _format_service_note(service_dimension: dict[str, object]) -> str:
    mode = service_dimension.get("mode")
    if mode == "claims_proxy":
        return "Servicios reales resueltos desde reclamos_zonificados para evitar la falsa vista de solo 2 servicios agregados."
    if mode == "direct":
        return f"Servicios filtrados directamente desde `{service_dimension.get('service_column')}`."
    return "No hay dimensión de servicio suficientemente rica para filtrar."


def _filter_related_claims(
    claims_df: pd.DataFrame,
    *,
    date_range: tuple[pd.Timestamp, pd.Timestamp] | None,
    selected_zones: list[str],
    selected_services: list[str],
) -> pd.DataFrame:
    filtered_claims = claims_df.copy()
    filtered_claims["fecha"] = pd.to_datetime(filtered_claims["fecha"], errors="coerce")

    if date_range is not None:
        start_ts, end_ts = date_range
        filtered_claims = filtered_claims.loc[filtered_claims["fecha"].between(start_ts, end_ts, inclusive="both")]

    if selected_zones:
        filtered_claims = filtered_claims.loc[filtered_claims["zona_id"].astype(str).isin(selected_zones)]

    if selected_services:
        service_columns = [column for column in ["servicio_normalizado", "servicio"] if column in filtered_claims.columns]
        if service_columns:
            mask = pd.Series(False, index=filtered_claims.index)
            normalized_selected = {service for service in (normalize_service_label(value) for value in selected_services) if service}
            for column in service_columns:
                mask = mask | filtered_claims[column].map(normalize_service_label).isin(normalized_selected)
            filtered_claims = filtered_claims.loc[mask]

    filtered_claims = filter_allowed_services(filtered_claims, service_columns=["servicio_normalizado", "servicio"])
    return filtered_claims.reset_index(drop=True)


def _apply_filters(df: pd.DataFrame, dataset_name: str, claims_df: pd.DataFrame) -> tuple[pd.DataFrame, str | None, dict[str, object]]:
    filtered = df.copy()
    date_column = detect_date_column(filtered)
    selected_zones: list[str] = []
    selected_services: list[str] = []
    date_range: tuple[pd.Timestamp, pd.Timestamp] | None = None
    if date_column is not None:
        filtered[date_column] = pd.to_datetime(filtered[date_column], errors="coerce")
        date_min = filtered[date_column].min()
        date_max = filtered[date_column].max()
        if pd.notna(date_min) and pd.notna(date_max):
            start_date, end_date = st.sidebar.date_input(
                "Rango de fechas",
                value=(date_min.date(), date_max.date()),
                min_value=date_min.date(),
                max_value=date_max.date(),
            )
            date_range = (pd.Timestamp(start_date), pd.Timestamp(end_date))
            filtered = filtered.loc[filtered[date_column].between(date_range[0], date_range[1], inclusive="both")]

    if "zona_id" in filtered.columns:
        zones = sorted(filtered["zona_id"].dropna().astype(str).unique().tolist())
        selected_zones = st.sidebar.multiselect("Zona", options=zones, default=[])
        if selected_zones:
            filtered = filtered.loc[filtered["zona_id"].astype(str).isin(selected_zones)]

    related_claims = _filter_related_claims(
        claims_df,
        date_range=date_range,
        selected_zones=selected_zones,
        selected_services=[],
    )
    allowed_services = get_allowed_services()
    service_dimension = resolve_service_dimension(filtered, dataset_name, related_claims_df=related_claims)
    filtered = filter_dataframe_by_services(filtered, service_dimension, allowed_services)
    service_dimension = resolve_service_dimension(filtered, dataset_name, related_claims_df=related_claims)
    service_frame = service_dimension.get("service_frame")
    if isinstance(service_frame, pd.DataFrame) and not service_frame.empty:
        services = [service for service in allowed_services if service in set(service_frame["service_label"].dropna().astype(str).tolist())]
        selected_services = st.sidebar.multiselect("Servicio", options=services, default=[])
        if selected_services:
            filtered = filter_dataframe_by_services(filtered, service_dimension, selected_services)

    if "llovio" in filtered.columns:
        rain_filter = st.sidebar.selectbox("Condición de lluvia", options=["Todas", "Llovió", "No llovió"], index=0)
        if rain_filter == "Llovió":
            filtered = filtered.loc[filtered["llovio"].fillna(False)]
        elif rain_filter == "No llovió":
            filtered = filtered.loc[~filtered["llovio"].fillna(False)]

    service_note = _format_service_note(service_dimension)
    claims_filtered = _filter_related_claims(
        claims_df,
        date_range=date_range,
        selected_zones=selected_zones,
        selected_services=selected_services,
    )

    ordered = filtered.sort_values(date_column, kind="stable") if date_column else filtered
    return ordered, date_column, {
        "date_range": date_range,
        "selected_zones": selected_zones,
        "selected_services": selected_services,
        "service_dimension": service_dimension,
        "service_note": service_note,
        "claims_filtered": claims_filtered,
    }


def _claim_marker_color(row: pd.Series) -> str:
    estado_geo = str(row.get("estado_geo", "")).strip().lower()
    if estado_geo == "ok":
        return "green"
    if estado_geo in {"aproximado", "estimado"}:
        return "orange"
    return "red"


def _format_popup_value(value: object, default: str = "—") -> str:
    if value is None or pd.isna(value):
        return default
    text = str(value).strip()
    return text or default


def _first_present(*values: object) -> object:
    for value in values:
        if value is not None and not pd.isna(value) and str(value).strip() != "":
            return value
    return None


def _render_claims_folium_map(claim_map_df: pd.DataFrame) -> None:
    center = [float(claim_map_df["lat"].median()), float(claim_map_df["lon"].median())]
    incidents_map = folium.Map(location=center, zoom_start=12, tiles="OpenStreetMap", control_scale=True)
    marker_cluster = MarkerCluster(disableClusteringAtZoom=15).add_to(incidents_map)

    for _, row in claim_map_df.iterrows():
        fecha = pd.to_datetime(row.get("fecha"), errors="coerce")
        popup_html = "".join(
            [
                '<div style="font-size:12px; min-width:240px">',
                f"<b>Reclamo:</b> {_format_popup_value(row.get('reclamo_id'))}<br>",
                f"<b>Fecha:</b> {fecha.strftime('%d/%m/%Y %H:%M') if pd.notna(fecha) else '—'}<br>",
                f"<b>Zona:</b> {_format_popup_value(row.get('zona_id'))}<br>",
                f"<b>Sede:</b> {_format_popup_value(row.get('sede_id'))}<br>",
                f"<b>Servicio:</b> {_format_popup_value(_first_present(row.get('servicio_normalizado'), row.get('servicio')))}<br>",
                f"<b>Motivo:</b> {_format_popup_value(row.get('motivo'))}<br>",
                f"<b>Estado geo:</b> {_format_popup_value(row.get('estado_geo'))}<br>",
                f"<b>Lat/Lon:</b> {row['lat']:.5f}, {row['lon']:.5f}<br>",
                "</div>",
            ]
        )
        folium.Marker(
            location=[row["lat"], row["lon"]],
            popup=folium.Popup(popup_html, max_width=320),
            tooltip=_format_popup_value(_first_present(row.get("servicio_normalizado"), row.get("servicio"))),
            icon=folium.Icon(color=_claim_marker_color(row), icon="info-sign"),
        ).add_to(marker_cluster)

    Fullscreen(position="topright", title="Pantalla completa", title_cancel="Salir", force_separate_button=True).add_to(incidents_map)
    incidents_map.fit_bounds(claim_map_df[["lat", "lon"]].values.tolist(), padding=(24, 24))
    st_folium(incidents_map, height=620, width='stretch', returned_objects=[])


def _render_zones_folium_map(zone_map_df: pd.DataFrame) -> None:
    center = [float(zone_map_df["lat"].median()), float(zone_map_df["lon"].median())]
    zone_map = folium.Map(location=center, zoom_start=11, tiles="CartoDB positron", control_scale=True)

    for _, row in zone_map_df.iterrows():
        popup_html = "".join(
            [
                '<div style="font-size:12px; min-width:220px">',
                f"<b>Zona:</b> {_format_popup_value(row.get('zona_id'))}<br>",
                f"<b>Días:</b> {_format_popup_value(row.get('dias'))}<br>",
                f"<b>Reclamos totales:</b> {_format_popup_value(row.get('reclamos_totales'))}<br>",
                f"<b>Servicio principal:</b> {_format_popup_value(row.get('servicio_principal'))}<br>",
                f"<b>Costo compuesto:</b> {_format_popup_value(row.get('costo_total_compuesto_ars'))}<br>",
                "</div>",
            ]
        )
        total_claims = row.get("reclamos_totales", 0)
        total_claims = 0 if pd.isna(total_claims) else float(total_claims)
        radius = 8 + min(total_claims, 50) * 0.35
        folium.CircleMarker(
            location=[row["lat"], row["lon"]],
            radius=radius,
            color="#1d4ed8",
            fill=True,
            fill_color="#60a5fa",
            fill_opacity=0.65,
            weight=2,
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"Zona {_format_popup_value(row.get('zona_id'))}",
        ).add_to(zone_map)

    zone_map.fit_bounds(zone_map_df[["lat", "lon"]].values.tolist(), padding=(20, 20))
    st_folium(zone_map, height=420, width='stretch', returned_objects=[])


def _show_summary_metrics(df: pd.DataFrame) -> None:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Filas filtradas", _format_number(len(df)))
    col2.metric("Zonas", _format_number(df["zona_id"].nunique()) if "zona_id" in df.columns else "—")
    col3.metric(
        "Reclamos totales",
        _format_number(df["reclamos_count"].sum()) if "reclamos_count" in df.columns else _format_number(df["reclamo_id"].nunique() if "reclamo_id" in df.columns else None),
    )
    col4.metric(
        "Costo total compuesto",
        _format_number(df["costo_total_compuesto_ars"].sum()) if "costo_total_compuesto_ars" in df.columns else "—",
    )


def _render_top_counts(df: pd.DataFrame, column: str, title: str, top_n: int = 10) -> None:
    if column not in df.columns or df[column].dropna().empty:
        st.info(f"No hay datos para **{title.lower()}**.")
        return

    top_counts = (
        df[column]
        .astype("string")
        .fillna("<NA>")
        .value_counts(dropna=False)
        .head(top_n)
        .rename_axis(column)
        .reset_index(name="count")
    )
    st.markdown(f"**{title}**")
    st.bar_chart(top_counts.set_index(column))
    st.dataframe(top_counts, use_container_width=True, hide_index=True)


def _render_summary_tab(filtered: pd.DataFrame, date_column: str | None, filter_context: dict[str, object]) -> None:
    st.caption(filter_context["service_note"])

    if date_column and not filtered.empty:
        metric_candidates = [column for column in DEFAULT_METRICS if column in filtered.columns]
        fallback_metrics = filtered.select_dtypes(include=["number", "bool"]).columns.tolist()
        available_metrics = metric_candidates or fallback_metrics
        if available_metrics:
            metric = st.selectbox("Métrica temporal", options=available_metrics)
            series = filtered.groupby(date_column, as_index=False)[metric].sum(numeric_only=False)
            st.line_chart(series.set_index(date_column))

    col1, col2 = st.columns(2)
    with col1:
        if "zona_id" in filtered.columns and not filtered.empty:
            top_zones = filtered["zona_id"].astype("string").value_counts().head(10).rename_axis("zona_id").reset_index(name="filas")
            st.markdown("**Zonas con más actividad**")
            st.bar_chart(top_zones.set_index("zona_id"))
            st.dataframe(top_zones, use_container_width=True, hide_index=True)
    with col2:
        related_claims = filter_context["claims_filtered"]
        if isinstance(related_claims, pd.DataFrame) and not related_claims.empty:
            service_summary = (
                related_claims["servicio_normalizado"]
                .astype("string")
                .value_counts()
                .head(10)
                .rename_axis("servicio")
                .reset_index(name="reclamos")
            )
            st.markdown("**Servicios observados en el contexto filtrado**")
            st.bar_chart(service_summary.set_index("servicio"))
            st.dataframe(service_summary, use_container_width=True, hide_index=True)


def _render_service_tab(filtered: pd.DataFrame, filter_context: dict[str, object]) -> None:
    related_claims = filter_context["claims_filtered"]
    st.caption(filter_context["service_note"])

    summary_source = related_claims if isinstance(related_claims, pd.DataFrame) and not related_claims.empty else filtered
    col1, col2 = st.columns(2)
    with col1:
        preferred_service_col = "servicio_normalizado" if "servicio_normalizado" in summary_source.columns else "zona_servicio_principal"
        _render_top_counts(summary_source, preferred_service_col, "Servicios", top_n=12)
    with col2:
        if "motivo" in summary_source.columns:
            _render_top_counts(summary_source, "motivo", "Motivos", top_n=12)
        elif "obs_categoria" in filtered.columns:
            _render_top_counts(filtered, "obs_categoria", "Categorías / observaciones", top_n=12)

    col3, col4 = st.columns(2)
    with col3:
        if "sede_id" in summary_source.columns:
            _render_top_counts(summary_source, "sede_id", "Sedes", top_n=10)
        elif "zona_id" in filtered.columns:
            _render_top_counts(filtered, "zona_id", "Zonas", top_n=10)
    with col4:
        if "zona_servicio_principal" in filtered.columns:
            _render_top_counts(filtered, "zona_servicio_principal", "Servicio principal agregado", top_n=10)
        elif "servicio" in summary_source.columns:
            _render_top_counts(summary_source, "servicio", "Servicio original", top_n=10)


def _render_maps_tab(filtered: pd.DataFrame, dataset_name: str, filter_context: dict[str, object]) -> None:
    related_claims = filter_context["claims_filtered"]
    st.subheader("Mapa de incidencias")
    claim_map_df = prepare_claim_map_dataset(related_claims)
    if claim_map_df.empty:
        st.info("No hay reclamos con coordenadas válidas para el contexto filtrado.")
    else:
        st.caption(
            f"Mapa principal con Folium, centrado automático, clusters y popups. Se muestran {_format_number(len(claim_map_df))} reclamos válidos del whitelist de servicios."
        )
        _render_claims_folium_map(claim_map_df)
        preview_columns = [column for column in ["reclamo_id", "fecha", "zona_id", "sede_id", "servicio_normalizado", "motivo"] if column in claim_map_df.columns]
        st.dataframe(claim_map_df[preview_columns].head(30), use_container_width=True, hide_index=True)

    st.subheader("Mapa de zonas")
    if dataset_name not in {"zona_diario_supervisado", "zona_diario_base"}:
        st.info("El mapa de zonas por centroides aplica a los datasets `zona_diario_*`.")
        return

    zone_map_df = prepare_zone_map_dataset(filtered)
    if zone_map_df.empty:
        st.info("No hay centroides válidos para las zonas filtradas.")
        return

    _render_zones_folium_map(zone_map_df)
    st.dataframe(
        zone_map_df.sort_values("reclamos_totales", ascending=False, kind="stable") if "reclamos_totales" in zone_map_df.columns else zone_map_df,
        use_container_width=True,
        hide_index=True,
    )


def _render_profile_tab(filtered: pd.DataFrame) -> None:
    st.subheader("Calidad básica")
    st.dataframe(quality_profile(filtered), use_container_width=True, hide_index=True)

    st.subheader("Variables numéricas")
    st.dataframe(numeric_profile(filtered), use_container_width=True, hide_index=True)

    categorical_candidates = [
        column
        for column in ["zona_id", "servicio_normalizado", "zona_servicio_principal", "lluvia_intensidad", "obs_categoria", "lluvia_status"]
        if column in filtered.columns
    ]
    if categorical_candidates:
        st.subheader("Top categorías")
        st.dataframe(categorical_profile(filtered, categorical_candidates), use_container_width=True, hide_index=True)


def main() -> None:
    st.title("Exploración interactiva del TP")
    st.caption("Vista académica, simple y defendible para explorar servicios reales, entender zonas y revisar calidad de los datasets del TP.")

    with st.expander("Inventario rápido de fuentes", expanded=False):
        st.dataframe(build_dataset_inventory(), use_container_width=True, hide_index=True)

    dataset_name = st.sidebar.selectbox(
        "Dataset",
        options=list(DATASET_LABELS),
        format_func=lambda value: DATASET_LABELS[value],
    )
    df = load_app_dataset(dataset_name)
    claims_df = load_app_dataset("reclamos_zonificados")
    filtered, date_column, filter_context = _apply_filters(df, dataset_name, claims_df)

    overview_df = dataset_overview(df, dataset_name)
    st.dataframe(overview_df, use_container_width=True, hide_index=True)
    _show_summary_metrics(filtered)

    tabs = st.tabs(["Resumen general", "Servicios / categorías", "Mapas", "Tabla filtrada", "Perfil / calidad"])

    with tabs[0]:
        _render_summary_tab(filtered, date_column, filter_context)

    with tabs[1]:
        _render_service_tab(filtered, filter_context)

    with tabs[2]:
        _render_maps_tab(filtered, dataset_name, filter_context)

    with tabs[3]:
        st.dataframe(filtered, use_container_width=True, hide_index=True)

    with tabs[4]:
        _render_profile_tab(filtered)


if __name__ == "__main__":
    main()
