import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta

# =========================================================
# UTILIDADES DE FECHA
# =========================================================

def to_date(x):
    """Convierte múltiples formatos a fecha estandarizada."""
    if pd.isna(x):
        return None
    s = str(x).strip()

    # YYYY/MM/DD
    if "/" in s and len(s.split("/")[0]) == 4:
        try: return datetime.strptime(s, "%Y/%m/%d").date()
        except: pass

    # DD-MM-YYYY
    if "-" in s and len(s.split("-")[2]) == 4 and len(s.split("-")[0]) <= 2:
        try: return datetime.strptime(s, "%d-%m-%Y").date()
        except: pass

    # MM/DD/YYYY
    if "/" in s and len(s.split("/")[2]) == 4:
        try: return datetime.strptime(s, "%m/%d/%Y").date()
        except: pass

    try: return pd.to_datetime(s).date()
    except: return None


# =========================================================
# NORMALIZAR ENCABEZADOS
# =========================================================

def normalize_headers(df):
    df.columns = (
        df.columns.astype(str)
        .str.replace("﻿", "")
        .str.replace('"', "")
        .str.strip()
    )
    return df


# =========================================================
# FILTRO DE RANGO DE FECHAS
# =========================================================

def filtrar_rango(df, col, d_from, d_to):
    if col not in df.columns:
        return df
    df = df[df[col].notna()]
    df = df[(df[col] >= d_from) & (df[col] <= d_to)]
    return df


# =========================================================
# PROCESO: VENTAS
# =========================================================

def process_ventas(df, agentes, d_from, d_to):
    df = normalize_headers(df.copy())

    df["fecha"] = df["date"].apply(to_date)
    df = filtrar_rango(df, "fecha", d_from, d_to)

    df["agente"] = df["ds_agent_email"]

    # Limpiar monto
    df["qt_price_local"] = (
        df["qt_price_local"]
        .astype(str)
        .str.replace(",", "")
        .str.replace("$", "")
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

    out = df.groupby(["agente", "fecha"], as_index=False)[
        ["Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas"]
    ].sum()

    return out.merge(agentes, left_on="agente", right_on="Email Cabify", how="right")


# =========================================================
# PROCESO: PERFORMANCE
# =========================================================

def process_performance(df, agentes, d_from, d_to):
    df = normalize_headers(df.copy())

    df = df[df["Group Support Service"] == "C_Ops Support"]

    df["fecha"] = df["Fecha de Referencia"].apply(to_date)
    df = filtrar_rango(df, "fecha", d_from, d_to)

    df["agente"] = df["Assignee Email"]
    df.loc[df["agente"].isna(), "agente"] = None  # Solo emails válidos

    convert = ["CSAT", "NPS Score", "Firt (h)", "Furt (h)", "% Firt", "% Furt"]
    for c in convert:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["Q_Encuestas"] = df.apply(
        lambda x: 1 if (not pd.isna(x["CSAT"]) or not pd.isna(x["NPS Score"])) else 0,
        axis=1,
    )

    df["Q_Tickets"] = 1
    df["Q_Tickets_Resueltos"] = df["Status"].apply(
        lambda x: 1 if str(x).strip().lower() == "solved" else 0
    )
    df["Q_Reopen"] = pd.to_numeric(df.get("Reopen", 0), errors="coerce").fillna(0)

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
        "Q_Tickets_Resueltos": "sum",
    })

    out = out.rename(columns={
        "NPS Score": "NPS",
        "Firt (h)": "FIRT",
        "% Firt": "%FIRT",
        "Furt (h)": "FURT",
        "% Furt": "%FURT"
    })

    return out.merge(agentes, left_on="agente", right_on="Email Cabify", how="right")


# =========================================================
# PROCESO: AUDITORÍAS
# =========================================================

def process_auditorias(df, agentes, d_from, d_to):

    df = normalize_headers(df.copy())

    df["fecha"] = df["Date Time"].apply(to_date)
    df = filtrar_rango(df, "fecha", d_from, d_to)

    # SOLO EMAILS VÁLIDOS
    df = df[df["Audited Agent"].astype(str).str.contains("@")]

    df["agente"] = df["Audited Agent"]

    df["Q_Auditorias"] = 1
    df["Nota_Auditorias"] = pd.to_numeric(df["Total Audit Score"], errors="coerce")

    out = df.groupby(["agente", "fecha"], as_index=False).agg({
        "Q_Auditorias": "sum",
        "Nota_Auditorias": "mean",
    })

    # Si un agente NO tiene auditorías → Q=0, Nota="-"
    out = out.merge(agentes, left_on="agente", right_on="Email Cabify", how="right")

    out["Q_Auditorias"] = out["Q_Auditorias"].fillna(0)
    out["Nota_Auditorias"] = out["Nota_Auditorias"].apply(
        lambda x: "-" if pd.isna(x) else x
    )

    return out


# =========================================================
# MATRIZ DIARIA
# =========================================================

def build_daily_matrix(dfs):

    merged = None
    for df in dfs:
        if df is not None and not df.empty:
            merged = df if merged is None else pd.merge(
                merged, df, on=["agente", "fecha", "Email Cabify",
                                "Tipo contrato", "Ingreso",
                                "Nombre", "Primer Apellido",
                                "Segundo Apellido", "Supervisor",
                                "Correo Supervisor"],
                how="outer"
            )

    if merged is None:
        return pd.DataFrame()

    merged = merged.sort_values(["fecha", "agente"])

    Q_cols = [
        "Q_Encuestas", "Q_Tickets", "Q_Tickets_Resueltos",
        "Q_Reopen", "Q_Auditorias",
        "Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas",
    ]

    for c in Q_cols:
        if c in merged.columns:
            merged[c] = merged[c].fillna(0)

    # Orden
    cols = [
        "fecha", "agente", "Tipo contrato", "Ingreso", "Nombre",
        "Primer Apellido", "Segundo Apellido", "Email Cabify",
        "Supervisor", "Correo Supervisor"
    ] + [c for c in merged.columns if c not in [
            "fecha", "agente", "Tipo contrato", "Ingreso", "Nombre",
            "Primer Apellido", "Segundo Apellido", "Email Cabify",
            "Supervisor", "Correo Supervisor"
        ]]

    return merged[cols]


# =========================================================
# MATRIZ SEMANAL
# =========================================================

def build_weekly_matrix(df_daily):

    if df_daily.empty:
        return pd.DataFrame()

    df = df_daily.copy()

    meses = {
        1: "Enero", 2: "Febrero", 3: "Marzo",
        4: "Abril", 5: "Mayo", 6: "Junio",
        7: "Julio", 8: "Agosto", 9: "Septiembre",
        10: "Octubre", 11: "Noviembre", 12: "Diciembre"
    }

    fecha_min = df["fecha"].min()
    inicio_sem = fecha_min - timedelta(days=fecha_min.weekday())

    def nombre_semana(f):
        delta = (f - inicio_sem).days
        nro = delta // 7
        ini = inicio_sem + timedelta(days=nro * 7)
        fin = ini + timedelta(days=6)
        return f"Semana {ini.day} al {fin.day} de {meses[fin.month]}"

    df["Semana"] = df["fecha"].apply(nombre_semana)

    agg = df.groupby(["Semana", "agente"], as_index=False).agg({
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
        "Ventas_Exclusivas": "sum"
    })

    agg = agg.merge(
        df_daily.drop_duplicates("agente")[
            ["agente", "Tipo contrato", "Ingreso", "Nombre",
             "Primer Apellido", "Segundo Apellido", "Email Cabify",
             "Supervisor", "Correo Supervisor"]
        ],
        on="agente",
        how="left"
    )

    # Sustituir promedios nulos
    prom = ["NPS", "CSAT", "FIRT", "%FIRT", "FURT", "%FURT", "Nota_Auditorias"]
    for c in prom:
        agg[c] = agg[c].apply(lambda x: "-" if pd.isna(x) else x)

    cols = ["Semana", "agente", "Tipo contrato", "Ingreso", "Nombre",
            "Primer Apellido", "Segundo Apellido", "Email Cabify",
            "Supervisor", "Correo Supervisor"] + [
                c for c in agg.columns if c not in [
                    "Semana", "agente", "Tipo contrato", "Ingreso", "Nombre",
                    "Primer Apellido", "Segundo Apellido", "Email Cabify",
                    "Supervisor", "Correo Supervisor"
                ]
            ]

    return agg[cols]


# =========================================================
# RESUMEN TOTAL
# =========================================================

def build_summary(df_daily):

    if df_daily.empty:
        return pd.DataFrame()

    agg = df_daily.groupby("agente", as_index=False).agg({
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
        "Ventas_Exclusivas": "sum"
    })

    agg = agg.merge(
        df_daily.drop_duplicates("agente")[
            ["agente", "Tipo contrato", "Ingreso", "Nombre", "Primer Apellido",
             "Segundo Apellido", "Email Cabify", "Supervisor", "Correo Supervisor"]
        ],
        on="agente",
        how="left"
    )

    prom = ["NPS", "CSAT", "FIRT", "%FIRT", "FURT", "%FURT", "Nota_Auditorias"]
    for c in prom:
        agg[c] = agg[c].apply(lambda x: "-" if pd.isna(x) else x)

    cols = ["agente", "Tipo contrato", "Ingreso", "Nombre", "Primer Apellido",
            "Segundo Apellido", "Email Cabify", "Supervisor", "Correo Supervisor"] + [
                c for c in agg.columns if c not in [
                    "agente", "Tipo contrato", "Ingreso", "Nombre", "Primer Apellido",
                    "Segundo Apellido", "Email Cabify", "Supervisor", "Correo Supervisor"
                ]
            ]

    return agg[cols]


# =========================================================
# FUNCIÓN PRINCIPAL
# =========================================================

def procesar_reportes(df_ventas, df_performance, df_auditorias, df_agentes,
                      date_from, date_to):

    agentes = normalize_headers(df_agentes.copy())

    diario_v = process_ventas(df_ventas, agentes, date_from, date_to)
    diario_p = process_performance(df_performance, agentes, date_from, date_to)
    diario_a = process_auditorias(df_auditorias, agentes, date_from, date_to)

    diario = build_daily_matrix([diario_v, diario_p, diario_a])
    semanal = build_weekly_matrix(diario)
    resumen = build_summary(diario)

    return {
        "diario": diario,
        "semanal": semanal,
        "resumen": resumen
    }
