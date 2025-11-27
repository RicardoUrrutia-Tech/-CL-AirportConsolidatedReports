import streamlit as st
import pandas as pd
from io import BytesIO
from processor import procesar_reportes

# ------------------------------------------------------------
# CONFIGURACIÓN DE LA APP
# ------------------------------------------------------------
st.set_page_config(page_title="Consolidador de Reportes - Aeropuerto", layout="wide")
st.title("🟦 Consolidador de Reportes – Aeropuerto Cabify")

st.markdown("""
Esta aplicación consolida los reportes de **Ventas**, **Performance** y **Auditorías**, 
generando matrices diarias, semanales y un resumen total por agente.
""")

# ------------------------------------------------------------
# SUBIDA DE ARCHIVOS
# ------------------------------------------------------------
st.header("📤 Cargar Archivos")

col1, col2 = st.columns(2)

with col1:
    ventas_file = st.file_uploader(
        "Reporte de Ventas (Excel .xlsx)", 
        type=["xlsx"], 
        key="ventas"
    )

with col2:
    performance_file = st.file_uploader(
        "Reporte de Performance (CSV)", 
        type=["csv"], 
        key="perf"
    )

auditorias_file = st.file_uploader(
    "Reporte de Auditorías (CSV - separador ;)  ", 
    type=["csv"], 
    key="aud"
)

# ------------------------------------------------------------
# BOTÓN PROCESAR
# ------------------------------------------------------------
if st.button("🔄 Procesar Reportes"):

    # Validación inicial
    if not ventas_file or not performance_file or not auditorias_file:
        st.error("❌ Debes cargar los 3 archivos para continuar.")
        st.stop()

    # ------------------------------------------------------------
    # LECTURA DE VENTAS
    # ------------------------------------------------------------
    try:
        df_ventas = pd.read_excel(ventas_file, engine="openpyxl")
    except Exception as e:
        st.error(f"❌ Error al leer Ventas: {e}")
        st.stop()

    # ------------------------------------------------------------
    # LECTURA DE PERFORMANCE (CSV)
    # ------------------------------------------------------------
    try:
        df_performance = pd.read_csv(
            performance_file,
            sep=",",
            engine="python",
            encoding="utf-8"
        )
    except Exception:
        try:
            df_performance = pd.read_csv(
                performance_file,
                sep=",",
                engine="python",
                encoding="latin-1"
            )
        except Exception as e:
            st.error(f"❌ Error al leer Performance: {e}")
            st.stop()

    # ------------------------------------------------------------
    # LECTURA DE AUDITORÍAS — FORMATO EXACTO DETECTADO
    # ------------------------------------------------------------
    try:
        auditorias_file.seek(0)  # Importante: resetear puntero
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
        st.error("❌ El archivo Auditorías no tiene columnas válidas.")
        st.stop()

    # ------------------------------------------------------------
    # PROCESAR REPORTES
    # ------------------------------------------------------------
    resultados = procesar_reportes(df_ventas, df_performance, df_auditorias)

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
    # DESCARGA DE ARCHIVO FINAL
    # ------------------------------------------------------------
    st.header("📥 Descargar Resultados")

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
        label="⬇ Descargar Excel Consolidado",
        data=excel_bytes,
        file_name="Reporte_Consolidado_Aeropuerto.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

else:
    st.info("Sube los archivos y presiona **Procesar Reportes** para continuar.")

