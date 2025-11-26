import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ---------------------------------------------------------
#  UTILIDADES
# ---------------------------------------------------------

def to_date(x):
    """
    Convierte múltiples formatos de fecha a datetime.date.
    Soporta formatos:
    - '2025-11-07'
    - '2025-11-25 02:01:26'
    - '11-25-2025'
    - '03-11-2025'
    - '2025/11/25'
    - '25/11/2025'
    """
    if pd.isna(x):
        return None

    formatos = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%m-%d-%Y",
        "%d-%m-%Y",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%m/%d/%Y"
    ]

    for fmt in formatos:
        try:
            return datetime.strptime(str(x), fmt).date()
        except:
            pass

    # fallback con pandas
    try:
        return pd.to_datetime(x).date()
    except:
        return None


# ---------------------------------------------------------
#  NORMALIZACIÓN DE AGENTES (correo)
# ---------------------------------------------------------

def build_email_mapping(df_ventas, df_inspecciones, df_auditorias):
    """
    Construye un mapa: nombre_solo → correo
    Para mapear performance cuando trae sólo FullName.
    """
    emails = []

    if df_ventas is not None:
        col = "ds_agent_email"
        if col in df_ventas.columns:
            emails.extend(df_ventas[col].dropna().unique().tolist())

    if df_inspecciones is not None:
        col = "Dirección de correo electrónico"
        if col in df_inspecciones.columns:
            emails.extend(df_inspecciones[col].dropna().unique().tolist())

    if df_auditorias is not None:
        col = "Audited Agent"
        if col in df_auditorias.columns:
            emails.extend(df_auditorias[col].dropna().unique().tolist())

    emails = list(set(emails))

    mapping = {}

    for mail in emails:
        name = mail.split("@")[0].replace(".", " ").lower().strip()
        mapping[name] = mail

    return mapping


def normalize_agent_name_to_email(agent_name, email_mapping):
    """
    Convierte 'KAREN MIRANDA' → karen.miranda@cabify.com
    usando el mapping construido.
    """
    if pd.isna(agent_name):
        return None

    key = str(agent_name).lower().strip().replace(".", " ")

    if key in email_mapping:
        return email_mapping[key]

    return None


# ---------------------------------------------------------
#  PROCESAMIENTO DE CADA REPORTE
# ---------------------------------------------------------

# 🟦 CORREGIDO — función robusta para Ventas
def process_ventas(df):
    if df is None:
        return pd.DataFrame()

    df = df.copy()

    # ------------------------------
    # FECHA
    # ------------------------------
    df["fecha"] = df["date"].apply(to_date)

    # ------------------------------
    # AGENTE
    # ------------------------------
    df["agente"] = df["ds_agent_email"]

    # ------------------------------
    # LIMPIEZA MONTOS (ROBUSTA)
    # ------------------------------
    if df["qt_price_local"].dtype == "O":  # texto u objeto
        df["qt_price_local"] = (
            df["qt_price_local"]
            .astype(str)
            .str.replace(",", "")
            .str.replace("$", "")
            .str.strip()
        )

    df["qt_price_local"] = pd.to_numeric(df["qt_price_local"], errors="coerce").fillna(0)

    # ------------------------------
    # INDICADORES
    # ------------------------------
    df["Ventas_Totales"] = df["qt_price_local"]

    df["Ventas_Compartidas"] = df.apply(
        lambda x: x["qt_price_local"] if str(x["ds_product_name"]) == "van_compartida" else 0,
        axis=1,
    )

    df["Ventas_Exclusivas"] = df.apply(
        lambda x: x["qt_price_local"] if str(x["ds_product_name"]) == "van_exclusiva" else 0,
        axis=1,
    )

    # ------------------------------
    # AGRUPAR POR AGENTE + FECHA
    # ------------------------------
    output = df.groupby(["agente", "fecha"], as_index=False)[
        ["Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas"]
    ].sum()

    return output


def process_performance(df, email_mapping):
    if df is None:
        return pd.DataFrame()

    df = df.copy()

    df = df[df["Group Support Service"] == "C_Ops Support"]

    df["fecha"] = df["Fecha de Referencia"].apply(to_date)

    df["agente"] = df["Assignee Email"]
    df.loc[df["agente"].isna(), "agente"] = df["Assignee FullName"].apply(
        lambda x: normalize_agent_name_to_email(x, email_mapping)
    )

    df["Q_Encuestas"] = df.apply(
        lambda x: 1 if (not pd.isna(x["CSAT"]) or not pd.isna(x["NPS Score"])) else 0, axis=1
    )

    df["Q_Tickets"] = 1

    df["Q_Tickets_Resueltos"] = df["Status"].apply(
        lambda x: 1 if str(x).lower().strip() == "solved" else 0
    )

    df["Q_Reopen"] = pd.to_numeric(df["Reopen"], errors="coerce").fillna(0)

    convertibles = ["CSAT", "NPS Score", "Firt (h)", "Furt (h)", "% Firt", "% Furt"]
    for col in convertibles:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    grouped = df.groupby(["agente", "fecha"], as_index=False).agg({
        "Q_Encuestas": "sum",
        "CSAT": "mean",
        "NPS Score": "mean",
        "Firt (h)": "mean",
        "% Firt": "mean",
        "Furt (h)": "mean",
        "% Furt": "mean",
        "Q_Reopen": "sum",
        "Q_Tickets": "sum",
        "Q_Tickets_Resueltos": "sum"
    })

    grouped = grouped.rename(columns={
        "NPS Score": "NPS",
        "Firt (h)": "FIRT",
        "% Firt": "%FIRT",
        "Furt (h)": "FURT",
        "% Furt": "%FURT",
    })

    return grouped


def process_inspecciones(df):
    if df is None:
        return pd.DataFrame()

    df = df.copy()
    df["fecha"] = df["Fecha"].apply(to_date)
    df["agente"] = df["Dirección de correo electrónico"]
    df["Q_Inspecciones"] = pd.to_numeric(df["N° Inspecciones"], errors="coerce").fillna(0)

    return df.groupby(["agente", "fecha"], as_index=False)["Q_Inspecciones"].sum()


def process_auditorias(df):
    if df is None:
        return pd.DataFrame()

    df = df.copy()
    df["fecha"] = df["Date Time"].apply(to_date)
    df["agente"] = df["Audited Agent"]

    df["Q_Auditorias"] = 1
    df["Nota_Auditorias"] = pd.to_numeric(df["Total Audit Score"], errors="coerce")

    return df.groupby(["agente", "fecha"], as_index=False).agg({
        "Q_Auditorias": "sum",
        "Nota_Auditorias": "mean"
    })


# ---------------------------------------------------------
#  MATRIZ DIARIA
# ---------------------------------------------------------

def build_daily_matrix(df_list):
    merged = None

    for df in df_list:
        if df is None or df.empty:
            continue

        if merged is None:
            merged = df
        else:
            merged = pd.merge(merged, df, on=["agente", "fecha"], how="outer")

    if merged is not None:
        for col in merged.columns:
            if col not in ["agente", "fecha"]:
                if merged[col].dtype in [np.float64, np.int64]:
                    merged[col] = merged[col].fillna(0)
                else:
                    merged[col] = merged[col].fillna("-")

    return merged


# ---------------------------------------------------------
#  MATRIZ SEMANAL
# ---------------------------------------------------------

def build_weekly_matrix(df_daily):
    df_daily = df_daily.copy()

    df_daily["semana"] = df_daily["fecha"].apply(
        lambda x: datetime.fromisoformat(str(x)).isocalendar().week
    )

    semanas = df_daily.groupby(["agente", "semana"], as_index=False).agg({
        "Q_Encuestas": "sum",
        "NPS": "mean",
        "CSAT": "mean",
        "FIRT": "mean",
        "%FIRT": "mean",
        "FURT": "mean",
        "%FURT": "mean",
        "Q_Reopen": "sum",
        "Q_Tickets": "sum",
        "Q_Tickets_Resueltos": "sum",
        "Q_Auditorias": "sum",
        "Nota_Auditorias": "mean",
        "Ventas_Totales": "sum",
        "Ventas_Compartidas": "sum",
        "Ventas_Exclusivas": "sum",
        "Q_Inspecciones": "sum"
    })

    return semanas


# ---------------------------------------------------------
#  RESUMEN TOTAL POR AGENTE
# ---------------------------------------------------------

def build_summary(df_daily):
    summary = df_daily.groupby("agente", as_index=False).agg({
        "Q_Encuestas": "sum",
        "NPS": "mean",
        "CSAT": "mean",
        "FIRT": "mean",
        "%FIRT": "mean",
        "FURT": "mean",
        "%FURT": "mean",
        "Q_Reopen": "sum",
        "Q_Tickets_Resueltos": "sum",
        "Q_Auditorias": "sum",
        "Nota_Auditorias": "mean",
        "Ventas_Totales": "sum",
        "Ventas_Compartidas": "sum",
        "Ventas_Exclusivas": "sum",
        "Q_Inspecciones": "sum"
    })
    return summary


# ---------------------------------------------------------
#  FUNCIÓN PRINCIPAL PARA STREAMLIT
# ---------------------------------------------------------

def procesar_reportes(df_ventas, df_performance, df_inspecciones, df_auditorias):

    email_mapping = build_email_mapping(df_ventas, df_inspecciones, df_auditorias)

    ventas = process_ventas(df_ventas)
    performance = process_performance(df_performance, email_mapping)
    inspecciones = process_inspecciones(df_inspecciones)
    auditorias = process_auditorias(df_auditorias)

    daily = build_daily_matrix([ventas, performance, inspecciones, auditorias])
    weekly = build_weekly_matrix(daily)
    summary = build_summary(daily)

    return {
        "diario": daily,
        "semanal": weekly,
        "resumen": summary
    }
