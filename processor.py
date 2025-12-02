# ===============================================================
#   ⬛⬛⬛   PROCESSOR.PY FINAL — CON DEBUG PARA ENCABEZADOS
# ===============================================================

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# =========================================================
# DEBUG: MOSTRAR ENCABEZADOS ORIGINALES Y NORMALIZADOS
# =========================================================
def debug_headers(df, titulo):
    """Devuelve un resumen con los encabezados antes/después de normalizar."""
    originales = list(df.columns.astype(str))

    df_norm = df.copy()
    df_norm = normalize_headers(df_norm)
    normalizados = list(df_norm.columns)

    return {
        "titulo": titulo,
        "headers_originales": originales,
        "headers_normalizados": normalizados,
        "diff_headers": list(set(originales) - set(normalizados)),
        "tiene_fecha_de_referencia": "fecha_de_referencia" in normalizados,
    }


# =========================================================
# NORMALIZACIÓN DE ENCABEZADOS (ULTRA ROBUSTA)
# =========================================================
def normalize_headers(df):
    cols = df.columns.astype(str)

    # Quitar BOMs comunes
    BOM_CHARS = ["﻿", "\ufeff", "ï»¿"]
    for c in BOM_CHARS:
        cols = cols.str.replace(c, "", regex=False)

    # Quitar TODOS los espacios unicode no estándar
    UNICODE_SPACES = [
        "\u00A0",  # NBSP
        "\u1680", "\u180E", "\u2000", "\u2001", "\u2002", "\u2003",
        "\u2004", "\u2005", "\u2006", "\u2007", "\u2008", "\u2009",
        "\u200A", "\u202F", "\u205F", "\u3000", "\u200B"
    ]
    for s in UNICODE_SPACES:
        cols = cols.str.replace(s, " ", regex=False)

    cols = cols.str.strip()

    # Normalizar acentos
    cols = (
        cols.str.normalize("NFKD")
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
    )

    cols = cols.str.lower()

    # Reemplazar cualquier símbolo por "_"
    cols = cols.str.replace(r"[^a-z0-9]+", "_", regex=True)
    cols = cols.str.replace(r"_+", "_", regex=True)
    cols = cols.str.strip("_")

    df.columns = cols
    return df


# =========================================================
# UTILIDADES DE FECHAS
# =========================================================

def to_date(x):
    if pd.isna(x):
        return None

    s = str(x).strip()

    if isinstance(x, (int, float)) and x > 30000:
        try:
            return (datetime(1899, 12, 30) + timedelta(days=float(x))).date()
        except:
            pass

    try:
        return pd.to_datetime(s).date()
    except:
        return None


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

def process_ventas(df, d_from, d_to, debug_list):
    debug_list.append(debug_headers(df, "VENTAS — antes de normalizar"))
    df = normalize_headers(df.copy())
    debug_list.append(debug_headers(df, "VENTAS — después de normalizar"))

    if "date" not in df.columns:
        return pd.DataFrame()

    df["fecha"] = df["date"].apply(to_date)
    df = filtrar_rango(df, "fecha", d_from, d_to)

    if "ds_agent_email" not in df.columns:
        return pd.DataFrame()

    df["agente"] = df["ds_agent_email"]

    if "qt_price_local" in df.columns:
        df["qt_price_local"] = (
            df["qt_price_local"]
            .astype(str)
            .str.replace(",", "")
            .str.replace("$", "")
            .str.replace(".", "")
            .str.strip()
        )

        df["qt_price_local"] = pd.to_numeric(df["qt_price_local"], errors="coerce").fillna(0)
    else:
        df["qt_price_local"] = 0

    df["ventas_totales"] = df["qt_price_local"]
    df["ventas_compartidas"] = 0
    df["ventas_exclusivas"] = 0

    if "ds_product_name" in df.columns:
        df["ventas_compartidas"] = df.apply(
            lambda x: x["qt_price_local"]
            if str(x["ds_product_name"]).lower().strip() == "van_compartida"
            else 0,
            axis=1,
        )
        df["ventas_exclusivas"] = df.apply(
            lambda x: x["qt_price_local"]
            if str(x["ds_product_name"]).lower().strip() == "van_exclusive"
            else 0,
            axis=1,
        )

    return df.groupby(["agente", "fecha"], as_index=False)[
        ["ventas_totales", "ventas_compartidas", "ventas_exclusivas"]
    ].sum()


# =========================================================
# PROCESO PERFORMANCE (CON DEBUG)
# =========================================================

def process_performance(df, d_from, d_to, debug_list):
    debug_list.append(debug_headers(df, "PERFORMANCE — antes de normalizar"))
    df = normalize_headers(df.copy())
    debug_list.append(debug_headers(df, "PERFORMANCE — después de normalizar"))

    # Confirmar columna de fecha
    posibles_fecha = [
        "fecha_de_referencia",
        "fecha_referencia",
        "reference_date",
        "created_at_local_dt",
        "created_at_local_dttm",
    ]

    fecha_col = next((c for c in posibles_fecha if c in df.columns), None)

    debug_list.append({
        "titulo": "DETECCIÓN COLUMNA FECHA PERFORMANCE",
        "fecha_detectada": fecha_col,
        "columnas": list(df.columns)
    })

    if fecha_col is None:
        raise Exception("No se encontró columna de fecha en Performance.")

    df["fecha"] = df[fecha_col].apply(to_date)
    df = filtrar_rango(df, "fecha", d_from, d_to)

    if "assignee_email" not in df.columns:
        raise Exception("No se encontró 'Assignee Email' en Performance normalizado.")

    df["agente"] = df["assignee_email"]

    df["q_encuestas"] = df.apply(
        lambda x: 1 if (not pd.isna(x.get("csat")) or not pd.isna(x.get("nps_score"))) else 0,
        axis=1,
    )

    df["q_tickets"] = 1
    df["q_tickets_resueltos"] = df["status"].apply(
        lambda x: 1 if str(x).strip().lower() == "solved" else 0
    )
    df["q_reopen"] = pd.to_numeric(df.get("reopen", 0), errors="coerce").fillna(0)

    for c in ["csat", "nps_score", "firt_h", "furt_h"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    out = df.groupby(["agente", "fecha"], as_index=False).agg({
        "q_encuestas": "sum",
        "csat": "mean",
        "nps_score": "mean",
        "firt_h": "mean",
        "furt_h": "mean",
        "q_reopen": "sum",
        "q_tickets": "sum",
        "q_tickets_resueltos": "sum",
    })

    out = out.rename(columns={
        "nps_score": "NPS",
        "firt_h": "FIRT",
        "furt_h": "FURT",
    })

    return out


# =========================================================
# PROCESO AUDITORÍAS
# =========================================================

def process_auditorias(df, d_from, d_to, debug_list):
    debug_list.append(debug_headers(df, "AUDITORÍAS — antes de normalizar"))
    df = normalize_headers(df.copy())
    debug_list.append(debug_headers(df, "AUDITORÍAS — después de normalizar"))

    if "date_time" not in df.columns:
        return pd.DataFrame()

    df["fecha"] = df["date_time"].apply(to_date)
    df = filtrar_rango(df, "fecha", d_from, d_to)

    if "audited_agent" not in df.columns:
        return pd.DataFrame()

    df = df[df["audited_agent"].astype(str).str.contains("@")]

    df["agente"] = df["audited_agent"]
    df["q_auditorias"] = 1
    df["nota_auditorias"] = pd.to_numeric(df.get("total_audit_score", 0), errors="coerce")

    return df.groupby(["agente", "fecha"], as_index=False).agg(
        {"q_auditorias": "sum", "nota_auditorias": "mean"}
    )


# =========================================================
# MERGE CON LISTADO DE AGENTES
# =========================================================

def merge_agentes(df, agentes_df, debug_list):
    debug_list.append(debug_headers(agentes_df, "AGENTES — antes de normalizar"))
    agentes_df = normalize_headers(agentes_df.copy())
    debug_list.append(debug_headers(agentes_df, "AGENTES — después de normalizar"))

    if df is None or df.empty:
        return df

    if "email_cabify" not in agentes_df.columns:
        raise Exception("No existe columna 'Email Cabify' en archivo de agentes.")

    df["agente"] = df["agente"].str.lower().str.strip()
    agentes_df["email_cabify"] = agentes_df["email_cabify"].str.lower().str.strip()

    df = df.merge(agentes_df, left_on="agente", right_on="email_cabify", how="left")

    return df[df["email_cabify"].notna()]


# =========================================================
# MATRIZ DIARIA
# =========================================================

def build_daily(df_list, agentes_df, debug_list):
    merged = None
    for df in df_list:
        if df is not None and not df.empty:
            merged = df if merged is None else pd.merge(
                merged, df, on=["agente", "fecha"], how="outer"
            )

    if merged is None or merged.empty:
        return pd.DataFrame()

    merged = merge_agentes(merged, agentes_df, debug_list)

    merged = merged.sort_values(["fecha", "agente"])

    for c in merged.columns:
        if merged[c].dtype in ["float64", "int64"]:
            merged[c] = merged[c].fillna(0)

    return merged


# =========================================================
# SEMANAL
# =========================================================

def build_weekly(df_daily):
    if df_daily is None or df_daily.empty:
        return pd.DataFrame()

    df = df_daily.copy()
    df["fecha"] = pd.to_datetime(df["fecha"])

    fecha_min = df["fecha"].min()
    inicio_sem = fecha_min - timedelta(days=fecha_min.weekday())

    def nombre_semana(f):
        delta = (f - inicio_sem).days
        sem = delta // 7
        ini = inicio_sem + timedelta(days=sem * 7)
        fin = ini + timedelta(days=6)
        meses = ["Enero","Febrero","Marzo","Abril","Mayo","Junio","Julio",
                 "Agosto","Septiembre","Octubre","Noviembre","Diciembre"]
        return f"Semana {ini.day} al {fin.day} de {meses[fin.month - 1]}"

    df["Semana"] = df["fecha"].apply(nombre_semana)

    if "email_cabify" in df.columns:
        df["agente_key"] = df["email_cabify"].str.lower().str.strip()
    else:
        df["agente_key"] = ""

    agg = {
        "q_encuestas": "sum",
        "NPS": "mean",
        "csat": "mean",
        "FIRT": "mean",
        "FURT": "mean",
        "q_reopen": "sum",
        "q_tickets": "sum",
        "q_tickets_resueltos": "sum",
        "q_auditorias": "sum",
        "nota_auditorias": "mean",
        "ventas_totales": "sum",
        "ventas_compartidas": "sum",
        "ventas_exclusivas": "sum",
    }

    weekly = df.groupby(["agente_key", "Semana"], as_index=False).agg(agg)

    info_cols = [
        "nombre","primer_apellido","segundo_apellido",
        "email_cabify","tipo_contrato","ingreso","supervisor"
    ]

    info_df = df[["agente_key"] + info_cols].drop_duplicates()

    weekly = weekly.merge(info_df, on="agente_key", how="left")

    return weekly


# =========================================================
# RESUMEN AGENTE
# =========================================================

def build_summary(df_daily):
    if df_daily is None or df_daily.empty:
        return pd.DataFrame()

    df = df_daily.copy()

    if "email_cabify" in df.columns:
        df["agente_key"] = df["email_cabify"].str.lower().str.strip()
    else:
        df["agente_key"] = ""

    agg = {
        "q_encuestas": "sum",
        "NPS": "mean",
        "csat": "mean",
        "FIRT": "mean",
        "FURT": "mean",
        "q_reopen": "sum",
        "q_tickets_resueltos": "sum",
        "q_auditorias": "sum",
        "nota_auditorias": "mean",
        "ventas_totales": "sum",
        "ventas_compartidas": "sum",
        "ventas_exclusivas": "sum",
    }

    resumen = df.groupby("agente_key", as_index=False).agg(agg)

    info_cols = [
        "nombre","primer_apellido","segundo_apellido",
        "email_cabify","tipo_contrato","ingreso","supervisor"
    ]

    resumen = resumen.merge(df[["agente_key"] + info_cols].drop_duplicates(),
                            on="agente_key", how="left")

    return resumen


# =========================================================
# SEMANAL POR SUPERVISOR
# =========================================================

def build_supervisor_weekly(df_daily):
    if df_daily is None or df_daily.empty:
        return pd.DataFrame()

    df = df_daily.copy()
    df["fecha"] = pd.to_datetime(df["fecha"])

    fecha_min = df["fecha"].min()
    inicio_sem = fecha_min - timedelta(days=fecha_min.weekday())

    def nombre_semana(f):
        delta = (f - inicio_sem).days
        sem = delta // 7
        ini = inicio_sem + timedelta(days=sem * 7)
        fin = ini + timedelta(days=6)
        meses = ["Enero","Febrero","Marzo","Abril","Mayo","Junio","Julio",
                 "Agosto","Septiembre","Octubre","Noviembre","Diciembre"]
        return f"Semana {ini.day} al {fin.day} de {meses[fin.month - 1]}"

    df["Semana"] = df["fecha"].apply(nombre_semana)

    agg = {
        "q_encuestas": "sum",
        "NPS": "mean",
        "csat": "mean",
        "FIRT": "mean",
        "FURT": "mean",
        "q_reopen": "sum",
        "q_tickets": "sum",
        "q_tickets_resueltos": "sum",
        "q_auditorias": "sum",
        "nota_auditorias": "mean",
        "ventas_totales": "sum",
        "ventas_compartidas": "sum",
        "ventas_exclusivas": "sum",
    }

    return df.groupby(["supervisor", "Semana"], as_index=False).agg(agg)


# =========================================================
# RESUMEN SUPERVISOR
# =========================================================

def build_supervisor_summary(df_daily):
    if df_daily is None or df_daily.empty:
        return pd.DataFrame()

    df = df_daily.copy()

    agg = {
        "q_encuestas": "sum",
        "NPS": "mean",
        "csat": "mean",
        "FIRT": "mean",
        "FURT": "mean",
        "q_reopen": "sum",
        "q_tickets": "sum",
        "q_tickets_resueltos": "sum",
        "q_auditorias": "sum",
        "nota_auditorias": "mean",
        "ventas_totales": "sum",
        "ventas_compartidas": "sum",
        "ventas_exclusivas": "sum",
    }

    return df.groupby("supervisor", as_index=False).agg(agg)


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
    debug_list = []

    ventas = process_ventas(df_ventas, d_from, d_to, debug_list)
    perf   = process_performance(df_performance, d_from, d_to, debug_list)
    auds   = process_auditorias(df_auditorias, d_from, d_to, debug_list)

    diario      = build_daily([ventas, perf, auds], agentes_df, debug_list)
    semanal     = build_weekly(diario)
    resumen     = build_summary(diario)
    semanal_sup = build_supervisor_weekly(diario)
    resumen_sup = build_supervisor_summary(diario)

    return {
        "diario": diario,
        "semanal": semanal,
        "resumen": resumen,
        "semanal_supervisor": semanal_sup,
        "resumen_supervisor": resumen_sup,

        # 👇 LOG COMPLETO PARA DEBUG EN STREAMLIT
        "debug": debug_list,
    }

