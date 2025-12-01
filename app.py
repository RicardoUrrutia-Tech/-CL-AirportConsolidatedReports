import streamlit as st
import pandas as pd
from io import BytesIO
from processor import procesar_reportes

# ------------------------------------------------------------
# CONFIGURACIÓN GENERAL
# ------------------------------------------------------------
st.set_page_config(page_title="Consolidador de Reportes - Aeropuerto", layout="wide")
st.title("🟦 Consolidador de Reportes – Aeropuerto Cabify")

st.markdown("""
Esta aplicación consolida los reportes de **Ventas**, **Performance** y **Auditorías**, 
generando matrices **diarias**, **semanales** y un **resumen total por agente**.
Ahora puedes **seleccionar el rango de fechas** antes de procesar.
""")

# ------------------------------------------------------------
# SUBIDA DE ARCHIVOS
# ------------------------------------------------------------
st.header("📤 Cargar Archivos")

col1, col2 = st.columns(2)

with col1:
    ventas_file = st.file_uploader(
        "📘 Reporte de Ventas (.xlsx)", 
        type=["xlsx"], 
        key="ventas"
    )

with col2:
    performance_file = st.file_uploader(
        "📗 Reporte de Performance (.csv)", 
        type=["csv"], 
        key="perf"
    )

auditorias_file = st.file_uploader(
    "📙 Reporte de Auditorías (.csv, separador 😉) ", 
    type=["csv"], 
    key="aud"
)

# ------------------------------------------------------------
# SI FALTAN ARCHIVOS → NO SE MUESTRA LO SIGUIENTE
# ------------------------------------------------------------
if not ventas_file or not performance_file or not auditorias_file:
    st.info("⬆ Sube los **3 archivos** para continuar.")
    st.stop()

# ------------------------------------------------------------
# SELECCIÓN DE RANGO DE FECHAS
# ------------------------------------------------------------
st.header("📅 Seleccionar Rango de Fechas")

# Fecha mínima y máxima tentativas
min_default = pd.to_datetime("2025-01-01")
max_default = pd.to_datetime("2025-12-31")

date_from = st.date_input("🔽 Fecha inicio del análisis:", min_default)
date_to = st.date_input("🔼 Fecha fin del análisis:", max_default)

if date_from > date_to:
    st.error("❌ La fecha inicio no puede ser mayor que la fecha fin.")
    st.stop()

st.success(f"📌 Rango seleccionado: **{date_from} → {date_to}**")

# ------------------------------------------------------------
# BOTÓN PROCESAR – SOLO APARECE DESPUÉS DEL RANGO
# ------------------------------------------------------------
if st.button("🔄 Procesar Reportes con este Rango de Fechas"):

    # ------------------------------------------------------------
    # LECTURA DE ARCHIVO VENTAS
    # ------------------------------------------------------------
    try:
        df_ventas = pd.read_excel(ventas_file, engine="openpyxl")
    except Exception as e:
        st.error(f"❌ Error al leer Ventas: {e}")
        st.stop()

    # ------------------------------------------------------------
    # LECTURA PERFORMANCE CSV
    # ------------------------------------------------------------
    try:
        df_performance = pd.read_csv(
            performance_file,
            sep=",",
            encoding="utf-8",
            engine="python"
        )
    except Exception:
        try:
            df_performance = pd.read_csv(
                performance_file,
                sep=",",
                encoding="latin-1",
                engine="python"
            )
        except Exception as e:
            st.error(f"❌ Error al leer Performance: {e}")
            st.stop()

    # ------------------------------------------------------------
    # LECTURA AUDITORÍAS — CSV EXACTO
    # ------------------------------------------------------------
    try:
        auditorias_file.seek(0)
        df_auditorias = pd.read_csv(
            auditorias_file,
            sep=";",
            encoding="utf-8-sig",
            engine="python"
        )
    except Exception as e:
        st.error(f"❌ Error al leer Auditorías: {e}")
        st.stop()

    if df_auditorias.shape[1] == 0:
        st.error("❌ El archivo de Auditorías no tiene columnas válidas.")
        st.stop()

    # ------------------------------------------------------------
    # PROCESAR REPORTES
    # ------------------------------------------------------------
    try:
        resultados = procesar_reportes(
            df_ventas, 
            df_performance, 
            df_auditorias,
            date_from,
            date_to
        )
    except Exception as e:
        st.error(f"❌ Error al procesar datos: {e}")
        st.stop()

    df_diario = resultados["diario"]
    df_semanal = resultados["semanal"]
    df_resumen = resultados["resumen"]

    # ------------------------------------------------------------
    # MOSTRAR RESULTADOS
    # ------------------------------------------------------------
    st.success("✔ Reportes procesados correctamente.")

    st.header("📅 Reporte Diario")
    st.dataframe(df_diario, use_container_width=True)

    st.header("📆 Reporte Semanal")
    st.dataframe(df_semanal, use_container_width=True)

    st.header("📊 Resumen Total por Agente")
    st.dataframe(df_resumen, use_container_width=True)

    # ------------------------------------------------------------
    # DESCARGA DE ARCHIVO
    # ------------------------------------------------------------
    st.header("📥 Descargar Excel Consolidado")

    def to_excel_multiple(diario, semanal, resumen):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine="xlsxwriter")

        diario.to_excel(writer, sheet_name="Diario", index=False)
        semanal.to_excel(writer, sheet_name="Semanal", index=False)
        resumen.to_excel(writer, sheet_name="Resumen", index=False)

        writer.close()
        return output.getvalue()

    excel_bytes = to_excel_multiple(df_diario, df_semanal, df_resumen)

    st.download_button(
        label="⬇ Descargar Resultados en Excel",
        data=excel_bytes,
        file_name=f"Reporte_Aeropuerto_{date_from}_a_{date_to}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

else:
    st.info("Presiona **Procesar Reportes** cuando estés listo.")
