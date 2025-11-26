import pandas as pd
import numpy as np
from datetime import datetime

# =========================================================
# UTILIDADES GENERALES
# =========================================================

def to_date(x):
    """Convierte múltiples formatos de fecha a datetime.date."""
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

    for f in formatos:
        try:
            return datetime.strptime(str(x), f).date()
        except:
            pass

    try:
        return pd.to_datetime(x).date()
    except:
        return None


# =========================================================
# NORMALIZACIÓN DE ENCABEZADOS
# =========================================================

def normalize_headers(df):
    """Normaliza nombres de columnas para evitar KeyErrors."""
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.replace('"', '')
        .str.replace("﻿", "")       # BOM
        .str.replace("\t", " ")
        .str.replace("\n", "")
        .str.replace("  ", " ")
    )
    return df


# =========================================================
# MAPEO DE NOMBRES → EMAILS
# =========================================================

def build_email_mapping(df_ventas, df_inspecciones, df_auditorias):
    emails = []

    # Ventas
    if df_ventas is not None and "ds_agent_email" in df_ventas.columns:
        emails.extend(df_ventas["ds_agent_email"].dropna().unique())

    # Inspecciones
    if df_inspecciones is not None and "Dirección de correo electrónico" in df_inspecciones.columns:
        emails.extend(df_inspecciones["Dirección de correo electrónico"].dropna().unique())

    # Auditorías
    if df_auditorias is not None and "Audited Agent" in df_auditorias.columns:
        emails.extend(df_auditorias["Audited Agent"].dropna().unique())

    emails = list(set(emails))

    mapping = {}
    for mail in emails:
        key = mail.split("@")[0].replace(".", " ").lower()
        mapping[key] = mail

    return mapping


def normalize_agent_name_to_email(name, mapping):
    """Convierte 'KAREN MIRANDA' en 'karen.miranda@cabify.com'."""
    if pd.isna(name):
        return None
    key = str(name).lower().replace(".", " ").strip()
    return mapping.get(key, None)


# =========================================================
# PROCESAMIENTO DE VENTAS
# =========================================================

def process_ventas(df):
    if df is None:
        return pd.DataFrame()

    df = normalize_headers(df.copy())

    # Fecha
    df["fecha"] = df["date"].apply(to_date)

    # Agente
    df["agente"] = df["ds_agent_email"]

    # Limpiar monto
    if df["qt_price_local"].dtype == "O":
        df["qt_price_local"] = (
            df["qt_price_local"].astype(str)
            .str.replace(",", "")
            .str.replace("$", "")
            .str.strip()
        )

    df["qt_price_local"] = pd.to_numeric(df["qt_price_local"], errors="coerce").fillna(0)

    # Indicadores
    df["Ventas_Totales"] = df["qt_price_local"]
    df["Ventas_Compartidas"] = df.apply(
        lambda x: x["qt_price_local"] if str(x["ds_product_name"]) == "van_compartida" else 0,
        axis=1
    )
    df["Ventas_Exclusivas"] = df.apply(
        lambda x: x["qt_price_local"] if str(x["ds_product_name"]) == "van_exclusiva" else 0,
        axis=1
    )

    return df.groupby(["agente", "fecha"], as_index=False)[
        ["Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas"]
    ].sum()


# =========================================================
# PROCESAMIENTO DE PERFORMANCE
# =========================================================

def process_performance(df, mapping):
    if df is None:
        return pd.DataFrame()

    df = normalize_headers(df.copy())

    # Filtrar grupo correcto
    if "Group Support Service" not in df.columns:
        return pd.DataFrame()

    df = df[df["Group Support Service"] == "C_Ops Support"]

    # Fecha
    df["fecha"] = df["Fecha de Referencia"].apply(to_date)

    # Agente: Email o FullName
    df["agente"] = df["Assignee Email"]
    df.loc[df["agente"].isna(), "agente"] = df["Assignee FullName"].apply(
        lambda x: normalize_agent_name_to_email(x, mapping)
    )

    # Indicadores base
    df["Q_Encuestas"] = df.apply(
        lambda x: 1 if (not pd.isna(x.get("CSAT")) or not pd.isna(x.get("NPS Score"))) else 0,
        axis=1
    )
    df["Q_Tickets"] = 1

    df["Q_Tickets_Resueltos"] = df["Status"].apply(
        lambda x: 1 if str(x).strip().lower() == "solved" else 0
    )

    df["Q_Reopen"] = pd.to_numeric(df.get("Reopen", 0), errors="coerce").fillna(0)

    # Convertibles seguros
    convertibles = ["CSAT", "NPS Score", "Firt (h)", "Furt (h)", "% Firt", "% Furt"]
    for col in convertibles:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = np.nan

    # Agrupar
    out = df.groupby(["agente", "fecha"], as_index=False).agg({
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

    out = out.rename(columns={
        "NPS Score": "NPS",
        "Firt (h)": "FIRT",
        "% Firt": "%FIRT",
        "Furt (h)": "FURT",
        "% Furt": "%FURT",
    })

    return out


# =========================================================
# PROCESAMIENTO DE INSPECCIONES
# =========================================================

def process_inspecciones(df):
    if df is None:
        return pd.DataFrame()

    df = normalize_headers(df.copy())

    df["fecha"] = df["Fecha"].apply(to_date)
    df["agente"] = df["Dirección de correo electrónico"]
    df["Q_Inspecciones"] = pd.to_numeric(df["N° Inspecciones"], errors="coerce").fillna(0)

    return df.groupby(["agente", "fecha"], as_index=False)["Q_Inspecciones"].sum()


# =========================================================
# PROCESAMIENTO DE AUDITORÍAS
# =========================================================

def process_auditorias(df):
    if df is None:
        return pd.DataFrame()

    df = normalize_headers(df.copy())

    df["fecha"] = df["Date Time"].apply(to_date)
    df["agente"] = df["Audited Agent"]

    df["Q_Auditorias"] = 1
    df["Nota_Auditorias"] = pd.to_numeric(df["Total Audit Score"], errors="coerce")

    return df.groupby(["agente", "fecha"], as_index=False).agg({
        "Q_Auditorias": "sum",
        "Nota_Auditorias": "mean"
    })


# =========================================================
# MATRICES FINALES: DIARIA, SEMANAL Y RESUMEN
# =========================================================

def build_daily_matrix(dfs):
    merged = None

    for df in dfs:
        if df is None or df.empty:
            continue
        merged = df if merged is None else pd.merge(merged, df, on=["agente", "fecha"], how="outer")

    if merged is not None:
        for col in merged.columns:
            if col not in ["agente", "fecha"]:
                if merged[col].dtype in [np.float64, np.int64]:
                    merged[col] = merged[col].fillna(0)
                else:
                    merged[col] = merged[col].fillna("-")

    return merged


def build_weekly_matrix(df):
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["semana"] = df["fecha"].apply(lambda x: datetime.fromisoformat(str(x)).isocalendar().week)

    grouping = {
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
    }

    for col in grouping.keys():
        if col not in df.columns:
            df[col] = 0

    return df.groupby(["agente", "semana"], as_index=False).agg(grouping)


def build_summary(df):
    if df is None or df.empty:
        return pd.DataFrame()

    grouping = {
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
    }

    for col in grouping.keys():
        if col not in df.columns:
            df[col] = 0

    return df.groupby("agente", as_index=False).agg(grouping)


# =========================================================
# FUNCIÓN PRINCIPAL PARA LA APP STREAMLIT
# =========================================================

def procesar_reportes(df_ventas, df_performance, df_inspecciones, df_auditorias):

    mapping = build_email_mapping(df_ventas, df_inspecciones, df_auditorias)

    ventas = process_ventas(df_ventas)
    performance = process_performance(df_performance, mapping)
    inspecciones = process_inspecciones(df_inspecciones)
    auditorias = process_auditorias(df_auditorias)

    diario = build_daily_matrix([ventas, performance, inspecciones, auditorias])
    semanal = build_weekly_matrix(diario)
    resumen = build_summary(diario)

    return {
        "diario": diario,
        "semanal": semanal,
        "resumen": resumen
    }
