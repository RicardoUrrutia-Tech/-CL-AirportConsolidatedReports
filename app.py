import streamlit as st
import pandas as pd
from io import BytesIO
from processor import procesar_reportes, normalize_headers

st.set_page_config(page_title="CLAIRPORT – CMI por Agentes y Supervisores", layout="wide")

st.title("👤📊 CMI – Agentes y Supervisores (CLAIRPORT)")

# =====================================================
# 📥 LECTOR ROBUSTO DE CSV/EXCEL
# =====================================================
def read_any(uploaded):
    raw = uploaded.read()
    uploaded.seek(0)

    if uploaded.name.endswith(".csv"):
        try:
            text = raw.decode("latin-1").replace("ï»¿", "").replace("\ufeff", "")
            sep = ";" if text.count(";") > text.count(",") else ","
            df = pd.read_csv(BytesIO(raw), encoding="latin-1", sep=sep)
        except:
            df = pd.read_csv(BytesIO(raw))
    else:
        df = pd.read_excel(uploaded)

    # 🔥 NORMALIZAR ENCABEZADOS APENAS SE CARGA
    df = normalize_headers(df)
    return df

# =====================================================
# CARGA DE ARCHIVOS
# =====================================================
st.header("📥 Subir Archivos")

col1, col2 = st.columns(2)

with col1:
    ventas_file = st.file_uploader("🔵 Ventas", type=["csv", "xlsx"])
    performance_file = st.file_uploader("🟢 Performance", type=["csv"])
    auditorias_file = st.file_uploader("🟣 Auditorías", type=["csv"])

with col2:
    agentes_file = st.file_uploader("👥 Listado de Agentes", type=["xlsx"])
    date_from = st.date_input("📆 Desde")
    date_to = st.date_input("📆 Hasta")

st.divider()

if not all([ventas_file, performance_file, auditorias_file, agentes_file]):
    st.info("Sube todos los archivos para continuar…")
    st.stop()

if not date_from or not date_to:
    st.warning("Selecciona un rango de fechas.")
    st.stop()

date_from = pd.to_datetime(date_from)
date_to = pd.to_datetime(date_to)

# =====================================================
# 🚀 PROCESAR
# =====================================================

if st.button("🚀 Procesar CMI", type="primary"):
    try:
        df_ventas = read_any(ventas_file)
        df_perf = read_any(performance_file)
        df_auds = read_any(auditorias_file)
        df_agentes = read_any(agentes_file)
    except Exception as e:
        st.error(f"❌ Error leyendo archivos: {e}")
        st.stop()

    try:
        resultados = procesar_reportes(
            df_ventas,
            df_perf,
            df_auds,
            df_agentes,
            date_from,
            date_to,
        )
    except Exception as e:
        st.error(f"❌ Error al procesar: {e}")
        st.stop()

    st.success("✅ CMI generado correctamente")

    diario = resultados["diario"]
    semanal = resultados["semanal"]
    resumen = resultados["resumen"]
    semanal_sup = resultados["semanal_supervisor"]
    resumen_sup = resultados["resumen_supervisor"]

    # =====================================================
    # TABS
    # =====================================================
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📅 Diario (Agente)",
        "📆 Semanal (Agente)",
        "📊 Resumen Agente",
        "👥 Semanal por Supervisor",
        "⭐ Resumen por Supervisor"
    ])

    with tab1:
        st.subheader("📅 Diario por Agente")
        st.dataframe(diario, use_container_width=True)

    with tab2:
        st.subheader("📆 Semanal por Agente")
        st.dataframe(semanal, use_container_width=True)

    with tab3:
        st.subheader("📊 Resumen del Periodo – Agentes")
        st.dataframe(resumen, use_container_width=True)

    with tab4:
        st.subheader("👥 Semanal por Supervisor")
        st.dataframe(semanal_sup, use_container_width=True)

    with tab5:
        st.subheader("⭐ Resumen Global por Supervisor")
        st.dataframe(resumen_sup, use_container_width=True)

    # =====================================================
    # DESCARGA
    # =====================================================
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        diario.to_excel(writer, index=False, sheet_name="Diario_Agente")
        semanal.to_excel(writer, index=False, sheet_name="Semanal_Agente")
        resumen.to_excel(writer, index=False, sheet_name="Resumen_Agente")
        semanal_sup.to_excel(writer, index=False, sheet_name="Semanal_Supervisor")
        resumen_sup.to_excel(writer, index=False, sheet_name="Resumen_Supervisor")

    st.download_button(
        "💾 Descargar Excel Completo",
        data=output.getvalue(),
        file_name="CMI_Agentes_Supervisores.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
else:
    st.info("Carga todos los archivos, selecciona fechas y presiona **Procesar CMI**.")


