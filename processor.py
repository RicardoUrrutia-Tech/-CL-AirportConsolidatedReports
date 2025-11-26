# ===============================================================
#   ⬛⬛⬛   PROCESSOR.PY FINAL - VERSION ESTABLE 2025   ⬛⬛⬛
# ===============================================================

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# =========================================================
# UTILIDADES DE FECHAS
# =========================================================

def to_date(x):
    """Convierte fechas según el formato detectado en cada reporte."""
    if pd.isna(x):
        return None

    s = str(x).strip()

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

    # fallback
    try:
        return pd.to_datetime(s).date()
    except:
        return None


# =========================================================
# NORMALIZACIÓN DE ENCABEZADOS
# =========================================================

def normalize_headers(df):
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.replace('"', '')
        .str.replace("﻿", "")      # BOM
        .str.replace("\t", " ")
        .str.replace("\n", "")
        .str.replace("  ", " ")
    )
    return df


# =========================================================
# MAPPING DE EMAILS ENTRE REPORTES
# =========================================================

def build_email_mapping(df_ventas, df_inspecciones, df_auditorias):
    emails = []

    if df_ventas is not None and "ds_agent_email" in df_ventas.columns:
        emails.extend(df_ventas["ds_agent_email"].dropna().unique())

    if df_inspecciones is not None:
        col = "Dirección de correo electrónico"
        if col in df_inspecciones.columns:
            emails.extend(df_inspecciones[col].dropna().unique())

    if df_auditorias is not None and "Audited Agent" in df_auditorias.columns:
        emails.extend(df_auditorias["Audited Agent"].dropna().unique())

    emails = list(set(emails))

    # clave → nombre.apellido sin puntos
    mapping = {}
    for mail in emails:
        key = mail.split("@")[0].replace(".", " ").lower().strip()
        mapping[key] = mail

    return mapping


def normalize_agent_name_to_email(name, mapping):
    if pd.isna(name):
        return None
    key = str(name).lower().replace(".", " ").strip()
    return mapping.get(key, None)


# =========================================================
# PROCESO VENTAS
# =========================================================

def process_ventas(df):
    if df is None:
        return pd.DataFrame()

    df = normalize_headers(df.copy())

    df["fecha"] = df["date"].apply(to_date)
    df["agente"] = df["ds_agent_email"]

    if df["qt_price_local"].dtype == "O":
        df["qt_price_local"] = (
            df["qt_price_local"].astype(str)
            .str.replace(",", "")
            .str.replace("$", "")
            .str.strip()
        )

    df["qt_price_local"] = pd.to_numeric(df["qt_price_local"], errors="coerce").fillna(0)

    df["Ventas_Totales"] = df["qt_price_local"]

    df["Ventas_Compartidas"] = df.apply(
        lambda x: x["qt_price_local"]
        if str(x["ds_product_name"]).strip().lower() == "van_compartida"
        else 0,
        axis=1
    )

    # ✔ CORRECTO → van_exclusive
    df["Ventas_Exclusivas"] = df.apply(
        lambda x: x["qt_price_local"]
        if str(x["ds_product_name"]).strip().lower() == "van_exclusive"
        else 0,
        axis=1
    )

    return df.groupby(["agente", "fecha"], as_index=False)[
        ["Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas"]
    ].sum()


# =========================================================
# PROCESO PERFORMANCE (CORREGIDO)
# =========================================================

def process_performance(df, mapping):
    if df is None:
        return pd.DataFrame()

    df = normalize_headers(df.copy())

    if "Group Support Service" not in df.columns:
        return pd.DataFrame()

    df = df[df["Group Support Service"] == "C_Ops Support"]

    df["fecha"] = df["Fecha de Referencia"].apply(to_date)
    df["agente"] = df["Assignee Email"]

    df.loc[df["agente"].isna(), "agente"] = df["Assignee FullName"].apply(
        lambda x: normalize_agent_name_to_email(x, mapping)
    )

    df["Q_Encuestas"] = df.apply(
        lambda x: 1 if (not pd.isna(x.get("CSAT")) or not pd.isna(x.get("NPS Score"))) else 0,
        axis=1
    )
    df["Q_Tickets"] = 1

    df["Q_Tickets_Resueltos"] = df["Status"].apply(
        lambda x: 1 if str(x).strip().lower() == "solved" else 0
    )

    df["Q_Reopen"] = pd.to_numeric(df.get("Reopen", 0), errors="coerce").fillna(0)

    convertibles = ["CSAT", "NPS Score", "Firt (h)", "Furt (h)", "% Firt", "% Furt"]
    for col in convertibles:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = np.nan

    # AGRUPACIÓN CORRECTA (SIN “NNPS”)
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

    # RENOMBRAR FINAL
    out = out.rename(columns={
        "NPS Score": "NPS",
        "Firt (h)": "FIRT",
        "% Firt": "%FIRT",
        "Furt (h)": "FURT",
        "% Furt": "%FURT"
    })

    return out


# =========================================================
# PROCESO INSPECCIONES
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
# PROCESO AUDITORIAS
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
# MATRIZ DIARIA
# =========================================================

def build_daily_matrix(dfs):

    merged = None
    for df in dfs:
        if df is not None and not df.empty:
            merged = df if merged is None else pd.merge(
                merged, df, on=["agente", "fecha"], how="outer"
            )

    if merged is None:
        return pd.DataFrame()

    merged = merged.sort_values(["fecha", "agente"])

    # INDICADORES DE CANTIDAD → 0
    Q_cols = [
        "Q_Encuestas","Q_Tickets","Q_Tickets_Resueltos","Q_Reopen",
        "Q_Auditorias","Q_Inspecciones",
        "Ventas_Totales","Ventas_Compartidas","Ventas_Exclusivas"
    ]

    for col in Q_cols:
        if col in merged.columns:
            merged[col] = merged[col].fillna(0)

    # Indicadores promedio → conservar NaN (no cambiar a “–” aquí)

    cols = ["fecha", "agente"] + [c for c in merged.columns if c not in ["fecha", "agente"]]
    return merged[cols]


# =========================================================
# MATRIZ SEMANAL
# =========================================================

def build_weekly_matrix(df_daily):

    if df_daily is None or df_daily.empty:
        return pd.DataFrame()

    df = df_daily.copy()
    df = df.sort_values("fecha")

    fecha_min = df["fecha"].min()
    inicio_sem = fecha_min - timedelta(days=fecha_min.weekday())

    meses = {
        1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",
        7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"
    }

    def nombre_semana(fecha):
        delta = (fecha - inicio_sem).days
        sem = delta // 7
        ini = inicio_sem + timedelta(days=sem*7)
        fin = ini + timedelta(days=6)
        return f"Semana {ini.day} al {fin.day} de {meses[fin.month]}"

    df["Semana"] = df["fecha"].apply(nombre_semana)

    weekly = df.groupby(["agente","Semana"], as_index=False).agg({
        "Q_Encuestas":"sum",
        "NPS":"mean",
        "CSAT":"mean",
        "FIRT":"mean",
        "%FIRT":"mean",
        "FURT":"mean",
        "%FURT":"mean",
        "Q_Reopen":"sum",
        "Q_Tickets":"sum",
        "Q_Tickets_Resueltos":"sum",
        "Q_Auditorias":"sum",
        "Nota_Auditorias":"mean",
        "Ventas_Totales":"sum",
        "Ventas_Compartidas":"sum",
        "Ventas_Exclusivas":"sum",
        "Q_Inspecciones":"sum"
    })

    # Reemplazo Final → NaN → “–”
    prom_cols = ["NPS","CSAT","FIRT","%FIRT","FURT","%FURT","Nota_Auditorias"]
    for c in prom_cols:
        weekly[c] = weekly[c].apply(lambda x: "-" if pd.isna(x) else x)

    return weekly


# =========================================================
# RESUMEN TOTAL
# =========================================================

def build_summary(df_daily):

    if df_daily is None or df_daily.empty:
        return pd.DataFrame()

    summary = df_daily.groupby("agente", as_index=False).agg({
        "Q_Encuestas":"sum",
        "NPS":"mean",
        "CSAT":"mean",
        "FIRT":"mean",
        "%FIRT":"mean",
        "FURT":"mean",
        "%FURT":"mean",
        "Q_Reopen":"sum",
        "Q_Tickets_Resueltos":"sum",
        "Q_Auditorias":"sum",
        "Nota_Auditorias":"mean",
        "Ventas_Totales":"sum",
        "Ventas_Compartidas":"sum",
        "Ventas_Exclusivas":"sum",
        "Q_Inspecciones":"sum"
    })

    prom_cols = ["NPS","CSAT","FIRT","%FIRT","FURT","%FURT","Nota_Auditorias"]
    for c in prom_cols:
        summary[c] = summary[c].apply(lambda x: "-" if pd.isna(x) else x)

    return summary


# =========================================================
# FUNCIÓN PRINCIPAL
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

