# ===============================================================
#   ⬛⬛⬛   PROCESSOR.PY FINAL 2025 — AGENTES + SUPERVISORES
# ===============================================================

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# =========================================================
# UTILIDADES DE FECHAS
# =========================================================

def to_date(x):
    """Convierte cualquier valor a fecha real (date), ignorando horas y timestamps."""
    if pd.isna(x):
        return None

    s = str(x).strip()

    # Si viene como número Excel (float)
    if isinstance(x, (int, float)) and x > 30000:
        try:
            return (datetime(1899, 12, 30) + timedelta(days=float(x))).date()
        except:
            pass

    # YYYY/MM/DD
    if "/" in s and len(s.split("/")[0]) == 4:
        try:
            return datetime.strptime(s, "%Y/%m/%d").date()
        except:
            pass

    # DD-MM-YYYY
    if "-" in s and len(s.split("-")[2]) == 4 and len(s.split("-")[0]) <= 2:
        try:
            return datetime.strptime(s, "%d-%m-%Y").date()
        except:
            pass

    # MM/DD/YYYY
    if "/" in s and len(s.split("/")[2]) == 4:
        try:
            return datetime.strptime(s, "%m/%d/%Y").date()
        except:
            pass

    # Último intento
    try:
        return pd.to_datetime(s).date()
    except:
        return None


# =========================================================
# LIMPIEZA DE COLUMNAS
# =========================================================

def normalize_headers(df):
    df.columns = (
        df.columns.astype(str)
        .str.replace("﻿", "")
        .str.replace("\ufeff", "")
        .str.strip()
    )
    return df


# =========================================================
# FILTRO DE FECHAS
# =========================================================

def filtrar_rango(df, col, d_from, d_to):
    if col not in df.columns:
        return df

    df[col] = df[col].apply(to_date)
    df = df[df[col].notna()]
    df = df[(df[col] >= d_from.date()) & (df[col] <= d_to.date())]

    return df


# =========================================================
# PROCESO VENTAS
# =========================================================

def process_ventas(df, d_from, d_to):
    if df is None or df.empty:
        return pd.DataFrame()

    df = normalize_headers(df.copy())

    df["fecha"] = df["date"].apply(to_date)
    df = filtrar_rango(df, "fecha", d_from, d_to)

    df["agente"] = df["ds_agent_email"]

    df["qt_price_local"] = (
        df["qt_price_local"]
        .astype(str)
        .str.replace(",", "")
        .str.replace("$", "")
        .str.replace(".", "")
        .str.strip()
    )

    df["qt_price_local"] = pd.to_numeric(df["qt_price_local"], errors="coerce").fillna(0)

    df["Ventas_Totales"] = df["qt_price_local"]

    df["Ventas_Compartidas"] = df.apply(
        lambda x: x["qt_price_local"]
        if str(x["ds_product_name"]).lower().strip() == "van_compartida"
        else 0,
        axis=1,
    )

    df["Ventas_Exclusivas"] = df.apply(
        lambda x: x["qt_price_local"]
        if str(x["ds_product_name"]).lower().strip() == "van_exclusive"
        else 0,
        axis=1,
    )

    return df.groupby(["agente", "fecha"], as_index=False)[
        ["Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas"]
    ].sum()


# =========================================================
# PROCESO PERFORMANCE
# =========================================================

def process_performance(df, d_from, d_to):
    if df is None or df.empty:
        return pd.DataFrame()

    df = normalize_headers(df.copy())

    if "Group Support Service" not in df.columns:
        return pd.DataFrame()

    df = df[df["Group Support Service"] == "C_Ops Support"]

    df["fecha"] = df["Fecha de Referencia"].apply(to_date)
    df = filtrar_rango(df, "fecha", d_from, d_to)

    df["agente"] = df["Assignee Email"]

    df["Q_Encuestas"] = df.apply(
        lambda x: 1 if (not pd.isna(x.get("CSAT")) or not pd.isna(x.get("NPS Score"))) else 0,
        axis=1,
    )

    df["Q_Tickets"] = 1
    df["Q_Tickets_Resueltos"] = df["Status"].apply(lambda x: 1 if str(x).strip().lower() == "solved" else 0)
    df["Q_Reopen"] = pd.to_numeric(df.get("Reopen", 0), errors="coerce").fillna(0)

    conv = ["CSAT", "NPS Score", "Firt (h)", "Furt (h)", "% Firt", "% Furt"]
    for c in conv:
        df[c] = pd.to_numeric(df.get(c, np.nan), errors="coerce")

    out = df.groupby(["agente", "fecha"], as_index=False).agg(
        {
            "Q_Encuestas": "sum",
            "CSAT": "mean",
            "NPS Score": "mean",
            "Firt (h)": "mean",
            "% Firt": "mean",
            "Furt (h)": "mean",
            "% Furt": "mean",
            "Q_Reopen": "sum",
            "Q_Tickets": "sum",
            "Q_Tickets_Resueltos": "sum",
        }
    )

    out = out.rename(
        columns={
            "NPS Score": "NPS",
            "Firt (h)": "FIRT",
            "% Firt": "%FIRT",
            "Furt (h)": "FURT",
            "% Furt": "%FURT",
        }
    )

    return out


# =========================================================
# PROCESO AUDITORÍAS
# =========================================================

def process_auditorias(df, d_from, d_to):
    if df is None or df.empty:
        return pd.DataFrame()

    df = normalize_headers(df.copy())

    df["fecha"] = df["Date Time"].apply(to_date)
    df = filtrar_rango(df, "fecha", d_from, d_to)

    df = df[df["Audited Agent"].astype(str).str.contains("@")]

    df["agente"] = df["Audited Agent"]

    df["Q_Auditorias"] = 1
    df["Nota_Auditorias"] = pd.to_numeric(df["Total Audit Score"], errors="coerce")

    out = df.groupby(["agente", "fecha"], as_index=False).agg(
        {"Q_Auditorias": "sum", "Nota_Auditorias": "mean"}
    )

    if out.empty:
        return pd.DataFrame(columns=["agente", "fecha", "Q_Auditorias", "Nota_Auditorias"])

    out["Nota_Auditorias"] = out["Nota_Auditorias"].fillna(0)

    return out


# =========================================================
# MERGE CON LISTADO DE AGENTES
# =========================================================

def merge_agentes(df, agentes_df):
    if df is None or df.empty:
        return df

    agentes_df = normalize_headers(agentes_df.copy())
    agentes_df["Email Cabify"] = agentes_df["Email Cabify"].str.lower().str.strip()

    df["agente"] = df["agente"].str.lower().str.strip()

    df = df.merge(
        agentes_df,
        left_on="agente",
        right_on="Email Cabify",
        how="left",
    )

    df = df[df["Email Cabify"].notna()]

    return df


# =========================================================
# MATRIZ DIARIA
# =========================================================

def build_daily(df_list, agentes_df):
    merged = None
    for df in df_list:
        if df is not None and not df.empty:
            merged = df if merged is None else pd.merge(
                merged, df, on=["agente", "fecha"], how="outer"
            )

    if merged is None or merged.empty:
        return pd.DataFrame()

    merged = merge_agentes(merged, agentes_df)

    merged = merged.sort_values(["fecha", "agente"])

    q_cols = [
        "Q_Encuestas",
        "Q_Tickets",
        "Q_Tickets_Resueltos",
        "Q_Reopen",
        "Q_Auditorias",
        "Ventas_Totales",
        "Ventas_Compartidas",
        "Ventas_Exclusivas",
    ]

    for c in q_cols:
        if c in merged.columns:
            merged[c] = merged[c].fillna(0)

    info_cols = [
        "Nombre",
        "Primer Apellido",
        "Segundo Apellido",
        "Email Cabify",
        "Tipo contrato",
        "Ingreso",
        "Supervisor",
    ]

    cols = (
        ["fecha"]
        + [c for c in info_cols if c in merged.columns]
        + [c for c in merged.columns if c not in (["fecha", "agente"] + info_cols)]
    )

    merged = merged[cols]

    return merged


# =========================================================
# MATRIZ SEMANAL
# =========================================================

def build_weekly(df_daily):
    if df_daily is None or df_daily.empty:
        return pd.DataFrame()

    df = df_daily.copy()
    df["fecha"] = pd.to_datetime(df["fecha"])

    meses = {
        1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
        5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
        9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
    }

    fecha_min = df["fecha"].min()
    inicio_sem = fecha_min - timedelta(days=fecha_min.weekday())

    def nombre_semana(fecha):
        delta = (fecha - inicio_sem).days
        sem = delta // 7
        ini = inicio_sem + timedelta(days=sem * 7)
        fin = ini + timedelta(days=6)
        return f"Semana {ini.day} al {fin.day} de {meses[fin.month]}"

    df["Semana"] = df["fecha"].apply(nombre_semana)

    if "Email Cabify" in df.columns:
        df["agente_key"] = df["Email Cabify"].str.lower().str.strip()
    else:
        df["agente_key"] = ""

    agg = {
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
    }

    weekly = df.groupby(["agente_key", "Semana"], as_index=False).agg(agg)

    info_cols = [
        "Nombre", "Primer Apellido", "Segundo Apellido",
        "Email Cabify", "Tipo contrato", "Ingreso",
        "Supervisor"
    ]

    info_df = df[["agente_key"] + info_cols].drop_duplicates()

    weekly = weekly.merge(info_df, on="agente_key", how="left")

    cols = (
        ["Semana"]
        + [c for c in info_cols if c in weekly.columns]
        + [c for c in weekly.columns if c not in (["Semana", "agente_key"] + info_cols)]
    )

    return weekly[cols]


# =========================================================
# RESUMEN AGENTE
# =========================================================

def build_summary(df_daily):
    if df_daily is None or df_daily.empty:
        return pd.DataFrame()

    df = df_daily.copy()

    if "Email Cabify" in df.columns:
        df["agente_key"] = df["Email Cabify"].str.lower().str.strip()
    else:
        df["agente_key"] = ""

    agg = {
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
    }

    resumen = df.groupby("agente_key", as_index=False).agg(agg)

    info_cols = [
        "Nombre", "Primer Apellido", "Segundo Apellido",
        "Email Cabify", "Tipo contrato", "Ingreso",
        "Supervisor"
    ]

    info_df = df[["agente_key"] + info_cols].drop_duplicates()

    resumen = resumen.merge(info_df, on="agente_key", how="left")

    cols = (
        [c for c in info_cols if c in resumen.columns]
        + [c for c in resumen.columns if c not in (["agente_key"] + info_cols)]
    )

    return resumen[cols]


# =========================================================
# SEMANAL POR SUPERVISOR
# =========================================================

def build_supervisor_weekly(df_daily):
    if df_daily is None or df_daily.empty:
        return pd.DataFrame()

    df = df_daily.copy()
    df["fecha"] = pd.to_datetime(df["fecha"])

    meses = {
        1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
        5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
        9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
    }

    fecha_min = df["fecha"].min()
    inicio_sem = fecha_min - timedelta(days=fecha_min.weekday())

    def nombre_semana(fecha):
        delta = (fecha - inicio_sem).days
        sem = delta // 7
        ini = inicio_sem + timedelta(days=sem * 7)
        fin = ini + timedelta(days=6)
        return f"Semana {ini.day} al {fin.day} de {meses[fin.month]}"

    df["Semana"] = df["fecha"].apply(nombre_semana)

    sum_cols = [
        "Q_Encuestas",
        "Q_Reopen",
        "Q_Tickets",
        "Q_Tickets_Resueltos",
        "Q_Auditorias",
        "Ventas_Totales",
        "Ventas_Compartidas",
        "Ventas_Exclusivas",
    ]

    mean_cols = [
        "NPS",
        "CSAT",
        "FIRT",
        "%FIRT",
        "FURT",
        "%FURT",
        "Nota_Auditorias",
    ]

    agg = {c: "sum" for c in sum_cols if c in df.columns}
    agg.update({c: "mean" for c in mean_cols if c in df.columns})

    weekly_sup = df.groupby(["Supervisor", "Semana"], as_index=False).agg(agg)

    cols = ["Semana", "Supervisor"] + [c for c in weekly_sup.columns if c not in ["Semana", "Supervisor"]]

    return weekly_sup[cols]


# =========================================================
# RESUMEN POR SUPERVISOR
# =========================================================

def build_supervisor_summary(df_daily):
    if df_daily is None or df_daily.empty:
        return pd.DataFrame()

    df = df_daily.copy()

    sum_cols = [
        "Q_Encuestas",
        "Q_Reopen",
        "Q_Tickets",
        "Q_Tickets_Resueltos",
        "Q_Auditorias",
        "Ventas_Totales",
        "Ventas_Compartidas",
        "Ventas_Exclusivas",
    ]

    mean_cols = [
        "NPS",
        "CSAT",
        "FIRT",
        "%FIRT",
        "FURT",
        "%FURT",
        "Nota_Auditorias",
    ]

    agg = {c: "sum" for c in sum_cols if c in df.columns}
    agg.update({c: "mean" for c in mean_cols if c in df.columns})

    resumen_sup = df.groupby("Supervisor", as_index=False).agg(agg)

    cols = ["Supervisor"] + [c for c in resumen_sup.columns if c not in ["Supervisor"]]

    return resumen_sup[cols]


# =========================================================
# FUNCIÓN PRINCIPAL
# =========================================================

def procesar_reportes(
    df_ventas,
    df_performance,
    df_auditorias,
    agentes_df,
    d_from,
    d_to,
):

    ventas = process_ventas(df_ventas, d_from, d_to)
    perf = process_performance(df_performance, d_from, d_to)
    auds = process_auditorias(df_auditorias, d_from, d_to)

    diario = build_daily([ventas, perf, auds], agentes_df)
    semanal = build_weekly(diario)
    resumen = build_summary(diario)

    semanal_sup = build_supervisor_weekly(diario)
    resumen_sup = build_supervisor_summary(diario)

    return {
        "diario": diario,
        "semanal": semanal,
        "resumen": resumen,
        "semanal_supervisor": semanal_sup,
        "resumen_supervisor": resumen_sup,
    }

