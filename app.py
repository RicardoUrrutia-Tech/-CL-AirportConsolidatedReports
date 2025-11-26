import streamlit as st
import pandas as pd
from io import BytesIO
from processor import procesar_reportes   # 🟦 IMPORTANTE: usa el motor de consolidación

# -----------------------------------------------------------
# CONFIGURACIÓN DE LA APP
# -----------------------------------------------------------
st.set_page_config(page_title="Consolidador Operacional", layout="wide")
st.title("🟦 Consolidador de Reportes Operacionales – Cabify Airport")

st.markdown("""
Carga los 4 reportes oficiales para generar:
- Informe **Diario**
- Informe **Semanal**
- **Resumen Total**
""")

st.markdown("---")

# -----------------------------------------------------------
# 1) REPORTE DE VENTAS (EXCEL)
# -----------------------------------------------------------
st.subheader("1️⃣ Cargar reporte de **Ventas** (Excel)")
file_ventas = st.file_uploader("Archivo Excel de Ventas", type=["xlsx", "xls"], key="ventas")
df_ventas = None

if file_ventas:
    try:
        df_ventas = pd.read_excel(file_ventas)
        st.success("Ventas cargado correctamente.")
        st.dataframe(df_ventas.head())
    except Exception as e:
        st.error(f"❌ Error al cargar Ventas: {e}")

# -----------------------------------------------------------
# 2) REPORTE DE PERFORMANCE (CSV)
# -----------------------------------------------------------
st.subheader("2️⃣ Cargar reporte de **Performance de Atención** (CSV)")
file_performance = st.file_uploader("Archivo CSV de Performance", type=["csv"], key="performance")
df_performance = None

if file_performance:
    try:
        df_performance = pd.read_csv(file_performance, encoding="utf-8")
        st.success("Performance cargado correctamente.")
        st.dataframe(df_performance.head())
    except Exception as e:
        st.error(f"❌ Error al cargar Performance: {e}")

# -----------------------------------------------------------
# 3) REPORTE DE INSPECCIONES (EXCEL)
# -----------------------------------------------------------
st.subheader("3️⃣ Cargar reporte de **Inspecciones** (Excel)")
file_inspecciones = st.file_uploader("Archivo Excel de Inspecciones", type=["xlsx", "xls"], key="inspecciones")
df_inspecciones = None

if file_inspecciones:
    try:
        df_inspecciones = pd.read_excel(file_inspecciones)
        st.success("Inspecciones cargado correctamente.")
        st.dataframe(df_inspecciones.head())
    except Exception as e:
        st.error(f"❌ Error al cargar Inspecciones: {e}")

# -----------------------------------------------------------
# 4) REPORTE DE AUDITORÍAS (CSV)
# -----------------------------------------------------------
st.subheader("4️⃣ Cargar reporte de **Auditorías** (CSV)")
file_auditorias = st.file_uploader("Archivo CSV de Auditorías", type=["csv"], key="auditorias")
df_auditorias = None

if file_auditorias:
    try:
        df_auditorias = pd.read_csv(file_auditorias, encoding="utf-8")
        st.success("Auditorías cargado correctamente.")
        st.dataframe(df_auditorias.head())
    except Exception as e:
        st.error(f"❌ Error al cargar Auditorías: {e}")

st.markdown("---")

# -----------------------------------------------------------
# VALIDACIÓN GENERAL
# -----------------------------------------------------------
if not all([df_ventas is not None, df_performance is not None, df_inspecciones is not None, df_auditorias is not None]):
    st.warning("⚠️ Debes cargar los 4 archivos para continuar.")
    st.stop()

# -----------------------------------------------------------
# BOTÓN PARA PROCESAR
# -----------------------------------------------------------
if st.button("🚀 Procesar reportes y generar Excel final"):

    with st.spinner("Procesando información..."):

        # Ejecutar motor de consolidación
        resultados = procesar_reportes(df_ventas, df_performance, df_inspecciones, df_auditorias)

        diario = resultados["diario"]
        semanal = resultados["semanal"]
        resumen = resultados["resumen"]

        st.success("¡Procesamiento completado!")

        # Mostrar preview
        st.subheader("📅 Vista previa – Diario")
        st.dataframe(diario.head())

        st.subheader("📅 Vista previa – Semanal")
        st.dataframe(semanal.head())

        st.subheader("📘 Vista previa – Resumen Total")
        st.dataframe(resumen)

        # -----------------------------------------------------------
        # GENERAR EXCEL PARA DESCARGA
        # -----------------------------------------------------------
        def to_excel_multi(df_diario, df_semanal, df_resumen):
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df_diario.to_excel(writer, index=False, sheet_name="Diario")
                df_semanal.to_excel(writer, index=False, sheet_name="Semanal")
                df_resumen.to_excel(writer, index=False, sheet_name="Resumen")
            return output.getvalue()

        excel_data = to_excel_multi(diario, semanal, resumen)

        st.download_button(
            label="📥 Descargar Excel Final",
            data=excel_data,
            file_name="Reporte_Final_Operacional.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        st.success("📘 Archivo final listo para descargar.")

