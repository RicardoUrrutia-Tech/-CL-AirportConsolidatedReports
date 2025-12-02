import pandas as pd
import numpy as np
import unicodedata
from datetime import datetime, timedelta


# ============================================================
# 🔥 NORMALIZACIÓN DE ENCABEZADOS – ULTRA ROBUSTA
# ============================================================

def normalize_headers(df):
    def normalize(text):
        text = str(text).strip()

        # Eliminar BOM, caracteres invisibles
        text = text.replace("﻿", "").replace("\ufeff", "")

        # Convertir a minúsculas
        text = text.lower()

        # Normalizar acentos
        text = "".join(
            c for c in unicodedata.normalize("NFD", text)
            if unicodedata.category(c) != "Mn"
        )

        # Reemplazar símbolos por espacio
        for sym in ["-", "/", "\\", ".", ",", "(", ")", "[", "]", "{", "}", ":", ";"]:
            text = text.replace(sym, " ")

        # Quitar dobles espacios
        while "  " in text:
            text = text.replace("  ", " ")

        # Convertir espacios a snake_case
        text = text.replace(" ", "_")

        return text.strip("_")

    df.columns = [normalize(c) for c in df.columns]

    # Diccionario de equivalencias fuertes
    col_map = {
        "fecha_de_referencia": "fecha_de_referencia",
        "fecha_referencia": "fecha_de_referencia",
        "referencia_fecha": "fecha_de_referencia",
        "date_time": "date_time",
        "datetime": "date_time",
        "date": "date",
        "tm_start_local_at": "tm_start_local_at",
        "assignee_email": "assignee_email",
        "agente_email": "assignee_email",
        "email_asignado": "assignee_email",
        "audited_agent": "audited_agent",
        "total_audit_score": "total_audit_score",
        "ds_product_name": "ds_product_name",
        "qt_price_local": "qt_price_local",
        "email_cabify": "email_cabify",
        "correo_supervisor": "correo_supervisor",
        "supervisor": "supervisor",
    }

    # Aplicar equivalencias si existen
    df.columns = [col_map.get(c, c) for c in df.columns]

    return df


# ============================================================
# 🔧 CONVERTIR A FECHA REAL
# ============================================================

def to_date(x):
    if pd.isna(x):
        return None

    s = str(x).strip()

    # Excel serial
    if isinstance(x, (int, float)) and x > 30000:
        try:
            return (datetime(1899, 12, 30) + timedelta(days=float(x))).date()
        except:
            pass

    # YYYY/MM/DD
    try:
        return datetime.strptime(s, "%Y/%m/%d").date()
    except:
        pass

    # DD-MM-YYYY
    try:
        return datetime.strptime(s, "%d-%m-%Y").date()
    except:
        pass

    # MM/DD/YYYY
    try:
        return datetime.strptime(s, "%m/%d/%Y").date()
    except:
        pass

    # Último intento
    try:
        return pd.to_datetime(s).date()
    except:
        return None


# ============================================================
# 🔍 FILTRO DE RANGO DE FECHAS
# ============================================================

def filtrar_rango(df, col, d_from, d_to):
    if col not in df.columns:
        df[col] = None

    df[col] = df[col].apply(to_date)
    df = df[df[col].notna()]
    return df[(df[col] >= d_from.date()) & (df[col] <= d_to.date())]


# ============================================================
# 🟦 PROCESAR VENTAS
# ============================================================

def process_ventas(df, d_from, d_to):
    if df is None or df.empty:
        return pd.DataFrame()

    df = normalize_headers(df.copy())

    # Asegurar columnas
    for c in ["date", "ds_agent_email", "qt_price_local", "ds_product_name"]:
        if c not in df.columns:
            df[c] = None

    df["fecha"] = df["date"].apply(to_date)
    df = filtrar_rango(df, "fecha", d_from, d_to)

    df["agente"] = df["ds_agent_email"].astype(str).str.lower().str.strip()

    # Normalizar precio
    df["qt_price_local"] = (
        df["qt_price_local"]
        .astype(str)
        .str.replace(",", "")
        .str.replace("$", "")
        .str.replace(".", "")
        .str.strip()
    )
    df["qt_price_local"] = pd.to_numeric(df["qt_price_local"], errors="coerce").fillna(0)

    df["ventas_totales"] = df["qt_price_local"]
    df["ventas_compartidas"] = df.apply(
        lambda x: x["qt_price_local"] if x["ds_product_name"] == "van_compartida" else 0,
        axis=1,
    )
    df["ventas_exclusivas"] = df.apply(
        lambda x: x["qt_price_local"] if x["ds_product_name"] == "van_exclusive" else 0,
        axis=1,
    )

    return df.groupby(["agente", "fecha"], as_index=False)[
        ["ventas_totales", "ventas_compartidas", "ventas_exclusivas"]
    ].sum()


# ============================================================
# 🟩 PROCESAR PERFORMANCE
# ============================================================

def process_performance(df, d_from, d_to):
    if df is None or df.empty:
        return pd.DataFrame()

    df = normalize_headers(df.copy())

    # Columnas mínimas necesarias
    for c in ["group_support_service", "assignee_email", "fecha_de_referencia"]:
        if c not in df.columns:
            df[c] = None

    df = df[df["group_support_service"] == "C_Ops Support"]

    df["fecha"] = df["fecha_de_referencia"].apply(to_date)
    df = filtrar_rango(df, "fecha", d_from, d_to)

    df["agente"] = df["assignee_email"].astype(str).str.lower().str.strip()

    # Métricas
    df["q_encuestas"] = df.apply(
        lambda x: 1 if (not pd.isna(x.get("csat")) or not pd.isna(x.get("nps_score"))) else 0,
        axis=1,
    )

    df["q_tickets"] = 1
    df["q_tickets_resueltos"] = df["status"].apply(
        lambda x: 1 if str(x).strip().lower() == "solved" else 0
    )

    df["q_reopen"] = pd.to_numeric(df.get("reopen", 0), errors="coerce").fillna(0)

    # Convertir columnas
    numeric = ["csat", "nps_score", "firt_(h)", "furt_(h)", "percent_firt", "percent_furt"]
    for c in numeric:
        if c not in df.columns:
            df[c] = None
        df[c] = pd.to_numeric(df[c], errors="coerce")

    out = df.groupby(["agente", "fecha"], as_index=False).agg(
        {
            "q_encuestas": "sum",
            "csat": "mean",
            "nps_score": "mean",
            "firt_(h)": "mean",
            "percent_firt": "mean",
            "furt_(h)": "mean",
            "percent_furt": "mean",
            "q_reopen": "sum",
            "q_tickets": "sum",
            "q_tickets_resueltos": "sum",
        }
    )

    return out


# ============================================================
# 🟪 PROCESAR AUDITORÍAS
# ============================================================

def process_auditorias(df, d_from, d_to):
    if df is None or df.empty:
        return pd.DataFrame()

    df = normalize_headers(df.copy())

    for c in ["date_time", "audited_agent", "total_audit_score"]:
        if c not in df.columns:
            df[c] = None

    df["fecha"] = df["date_time"].apply(to_date)
    df = filtrar_rango(df, "fecha", d_from, d_to)

    df = df[df["audited_agent"].astype(str).str.contains("@", na=False)]

    df["agente"] = df["audited_agent"].astype(str).str.lower().str.strip()
    df["q_auditorias"] = 1
    df["nota_auditorias"] = pd.to_numeric(df["total_audit_score"], errors="coerce")

    out = df.groupby(["agente", "fecha"], as_index=False).agg(
        {"q_auditorias": "sum", "nota_auditorias": "mean"}
    )

    return out


# ============================================================
# 🧩 MERGE CON AGENTES
# ============================================================

def merge_agentes(df, agentes_df):
    if df is None or df.empty:
        return df

    agentes_df = normalize_headers(agentes_df.copy())
    agentes_df["email_cabify"] = agentes_df["email_cabify"].str.lower().str.strip()

    df["agente"] = df["agente"].str.lower().str.strip()

    df = df.merge(
        agentes_df,
        left_on="agente",
        right_on="email_cabify",
        how="left",
    )

    return df[df["email_cabify"].notna()]


# ============================================================
# 📅 DIARIO
# ============================================================

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

    # Rellenar nulos
    metrics = [
        "q_encuestas",
        "q_tickets",
        "q_tickets_resueltos",
        "q_reopen",
        "q_auditorias",
        "ventas_totales",
        "ventas_compartidas",
        "ventas_exclusivas",
    ]

    for c in metrics:
        if c in merged.columns:
            merged[c] = merged[c].fillna(0)

    merged = merged.sort_values(["fecha", "agente"])

    return merged


# ============================================================
# 📆 SEMANAL POR AGENTE
# ============================================================

def build_weekly(df):
    if df.empty:
        return pd.DataFrame()

    df["fecha"] = pd.to_datetime(df["fecha"])
    meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
             "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]

    fecha_min = df["fecha"].min()
    inicio_sem = fecha_min - timedelta(days=fecha_min.weekday())

    def nombre_semana(fecha):
        delta = (fecha - inicio_sem).days
        sem = delta // 7
        ini = inicio_sem + timedelta(days=sem * 7)
        fin = ini + timedelta(days=6)
        return f"Semana {ini.day} al {fin.day} de {meses[fin.month - 1]}"

    df["semana"] = df["fecha"].apply(nombre_semana)

    sum_cols = [
        "q_encuestas", "q_tickets", "q_tickets_resueltos",
        "q_reopen", "q_auditorias",
        "ventas_totales", "ventas_compartidas", "ventas_exclusivas"
    ]

    mean_cols = [
        "csat", "nps_score", "firt_(h)", "percent_firt",
        "furt_(h)", "percent_furt", "nota_auditorias"
    ]

    agg = {c: "sum" for c in sum_cols if c in df.columns}
    agg.update({c: "mean" for c in mean_cols if c in df.columns})

    weekly = df.groupby(["agente", "semana"], as_index=False).agg(agg)

    info_cols = [
        "nombre", "primer_apellido", "segundo_apellido",
        "email_cabify", "tipo_contrato", "ingreso", "supervisor"
    ]

    info_df = df[["agente"] + info_cols].drop_duplicates()

    weekly = weekly.merge(info_df, on="agente", how="left")

    return weekly


# ============================================================
# ⭐ RESUMEN AGENTE
# ============================================================

def build_summary(df):
    if df.empty:
        return pd.DataFrame()

    sum_cols = [
        "q_encuestas", "q_tickets_resueltos", "q_reopen",
        "q_auditorias", "ventas_totales",
        "ventas_compartidas", "ventas_exclusivas"
    ]

    mean_cols = [
        "csat", "nps_score", "firt_(h)", "percent_firt",
        "furt_(h)", "percent_furt", "nota_auditorias"
    ]

    agg = {c: "sum" for c in sum_cols if c in df.columns}
    agg.update({c: "mean" for c in mean_cols if c in df.columns})

    resumen = df.groupby("agente", as_index=False).agg(agg)

    info_cols = [
        "nombre", "primer_apellido", "segundo_apellido",
        "email_cabify", "tipo_contrato", "ingreso", "supervisor"
    ]

    info_df = df[["agente"] + info_cols].drop_duplicates()

    resumen = resumen.merge(info_df, on="agente", how="left")

    return resumen


# ============================================================
# 👥 SEMANAL POR SUPERVISOR
# ============================================================

def build_supervisor_weekly(df):
    if df.empty:
        return pd.DataFrame()

    df["fecha"] = pd.to_datetime(df["fecha"])

    meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
             "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]

    fecha_min = df["fecha"].min()
    inicio_sem = fecha_min - timedelta(days=fecha_min.weekday())

    def nombre_semana(fecha):
        delta = (fecha - inicio_sem).days
        sem = delta // 7
        ini = inicio_sem + timedelta(days=sem * 7)
        fin = ini + timedelta(days=6)
        return f"Semana {ini.day} al {fin.day} de {meses[fin.month - 1]}"

    df["semana"] = df["fecha"].apply(nombre_semana)

    sum_cols = [
        "q_encuestas", "q_tickets", "q_tickets_resueltos", "q_reopen",
        "q_auditorias", "ventas_totales", "ventas_compartidas", "ventas_exclusivas"
    ]

    mean_cols = [
        "csat", "nps_score", "firt_(h)", "percent_firt",
        "furt_(h)", "percent_furt", "nota_auditorias"
    ]

    agg = {c: "sum" for c in sum_cols if c in df.columns}
    agg.update({c: "mean" for c in mean_cols if c in df.columns})

    weekly_sup = df.groupby(["supervisor", "semana"], as_index=False).agg(agg)

    return weekly_sup


# ============================================================
# ⭐ RESUMEN SUPERVISOR
# ============================================================

def build_supervisor_summary(df):
    if df.empty:
        return pd.DataFrame()

    sum_cols = [
        "q_encuestas", "q_tickets", "q_tickets_resueltos", "q_reopen",
        "q_auditorias", "ventas_totales", "ventas_compartidas", "ventas_exclusivas"
    ]

    mean_cols = [
        "csat", "nps_score", "firt_(h)", "percent_firt",
        "furt_(h)", "percent_furt", "nota_auditorias"
    ]

    agg = {c: "sum" for c in sum_cols if c in df.columns}
    agg.update({c: "mean" for c in mean_cols if c in df.columns})

    resumen_sup = df.groupby("supervisor", as_index=False).agg(agg)

    return resumen_sup


# ============================================================
# 🚀 FUNCIÓN PRINCIPAL
# ============================================================

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
