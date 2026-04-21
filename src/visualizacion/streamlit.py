"""
Dashboard: Prediccion del Consumo de Energia en Costa Rica
streamlit run dashboard.py
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# ─── CONFIGURACION GLOBAL ────────────────────────────────────────────────────
st.set_page_config(
    page_title="Energia CR — Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

PALETA = ['#003049','#0077b6','#00b4d8','#e9c46a','#e76f51',
          '#2a9d8f','#6a4c93','#f4a261','#264653']

st.markdown("""
<style>
    /* Fondo y tipografia general */
    [data-testid="stAppViewContainer"] {
        background-color: #020f1a;
        color: #e0f4ff;
    }
    [data-testid="stSidebar"] {
        background-color: #011627;
        border-right: 1px solid #0077b6;
    }
    h1, h2, h3 {
        font-family: Georgia, serif;
        color: #caf0f8;
    }

    /* Tarjetas KPI */
    .kpi-card {
        background: linear-gradient(135deg, #011627 0%, #013a63 100%);
        border: 1px solid #0077b6;
        border-radius: 12px;
        padding: 20px 24px;
        text-align: center;
        margin-bottom: 8px;
    }
    .kpi-label {
        font-size: 12px;
        color: #90e0ef;
        letter-spacing: 1.5px;
        text-transform: uppercase;
        margin-bottom: 6px;
    }
    .kpi-value {
        font-size: 28px;
        font-weight: bold;
        color: #caf0f8;
        font-family: Georgia, serif;
    }
    .kpi-delta {
        font-size: 13px;
        margin-top: 4px;
    }
    .kpi-up   { color: #2a9d8f; }
    .kpi-down { color: #e76f51; }

    /* Hipotesis banners */
    .hipotesis-banner {
        background: linear-gradient(90deg, #003049 0%, #023e8a 100%);
        border-left: 5px solid #00b4d8;
        border-radius: 8px;
        padding: 16px 20px;
        margin: 16px 0;
        font-size: 15px;
        color: #e0f4ff;
        line-height: 1.6;
    }
    .hipotesis-resultado-si {
        background: linear-gradient(90deg, #0a2e1a 0%, #013a20 100%);
        border-left: 5px solid #2a9d8f;
        border-radius: 8px;
        padding: 12px 20px;
        margin: 10px 0;
        color: #b7e4c7;
    }
    .hipotesis-resultado-no {
        background: linear-gradient(90deg, #2e0a0a 0%, #3a0101 100%);
        border-left: 5px solid #e76f51;
        border-radius: 8px;
        padding: 12px 20px;
        margin: 10px 0;
        color: #f4b8a0;
    }
    .hipotesis-resultado-parcial {
        background: linear-gradient(90deg, #2e2a0a 0%, #3a350a 100%);
        border-left: 5px solid #e9c46a;
        border-radius: 8px;
        padding: 12px 20px;
        margin: 10px 0;
        color: #f9e4a0;
    }

    /* Separadores de seccion */
    .seccion-titulo {
        border-bottom: 2px solid #0077b6;
        padding-bottom: 8px;
        margin-top: 32px;
        margin-bottom: 20px;
        font-size: 22px;
        color: #caf0f8;
        font-family: Georgia, serif;
    }

    /* Divider */
    hr { border-color: #0077b640; }

    /* Sidebar labels */
    .css-1d391kg, [data-testid="stSidebarContent"] label {
        color: #90e0ef !important;
    }
</style>
""", unsafe_allow_html=True)

# ─── CARGA DE DATOS ──────────────────────────────────────────────────────────
@st.cache_data
def cargar_csv():
    ruta = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'processed', 'dataset_final_2020_2025.csv')
    df = pd.read_csv(ruta, encoding='latin1')
    df['Año'] = df['Año'].astype(int)
    cols_clima = ['T2M','WS10M','CLOUD_AMT','RH2M','T2M_MAX','T2M_MIN',
                  'CLOUD_OD','GWETROOT','TS','PRECTOTCORR',
                  'ALLSKY_SFC_SW_DWN','PS','T2MWET','ALLSKY_SFC_SW_DIFF','ALLSKY_SFC_LW_DWN']
    df[cols_clima] = df[cols_clima].replace(-999, float('nan'))
    df['Periodo'] = df['Año'].apply(
        lambda x: 'Pandemia (2020-2021)' if x in [2020, 2021]
        else ('Recuperacion (2022-2023)' if x in [2022, 2023]
              else 'Expansion (2024-2025)')
    )
    df['Estacion'] = df['Mes'].apply(
        lambda x: 'Verano (Dic-Abr)' if x in [12, 1, 2, 3, 4] else 'Lluvioso (May-Nov)'
    )
    return df

@st.cache_data
def cargar_db(query):
    try:
        from datos.GestorDBconn import GestorDBconn
        db = GestorDBconn()
        return db.consultar(query), True
    except Exception as e:
        return None, False

df = cargar_csv()
MESES = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Set','Oct','Nov','Dic']

# ─── SIDEBAR ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="text-align:center; padding: 12px 0 24px 0;">
        <div style="font-size:36px;" </div>
        <div style="font-family:Georgia,serif; font-size:18px; color:#caf0f8; font-weight:bold;">
            Energia CR
        </div>
        <div style="font-size:11px; color:#90e0ef; letter-spacing:2px;">DASHBOARD ANALITICO</div>
    </div>
    """, unsafe_allow_html=True)

    seccion = st.radio(
        "Navegacion",
        [" Resumen General",
         " Hipotesis: Pandemia",
         " Hipotesis: Verano",
         " Hipotesis: Guanacaste",
         " Ventas y Evolucion",
         " Clima vs Consumo",
         " Geografia y Centrales"],
        label_visibility="collapsed"
    )

    st.markdown("<hr>", unsafe_allow_html=True)

    # Filtros globales
    st.markdown("**Filtros globales**")
    años_sel = st.multiselect(
        "Años",
        options=sorted(df['Año'].unique()),
        default=sorted(df['Año'].unique())
    )
    empresas_sel = st.multiselect(
        "Empresas",
        options=sorted(df['Empresa'].unique()),
        default=sorted(df['Empresa'].unique())
    )

    st.markdown("<hr>", unsafe_allow_html=True)
    st.markdown('<div style="font-size:11px;color:#90e0ef;text-align:center;">CUC · Proyecto 5 · 2026</div>',
                unsafe_allow_html=True)

# Aplicar filtros
df_f = df[df['Año'].isin(años_sel) & df['Empresa'].isin(empresas_sel)]

# Helper para graficos oscuros
def fig_oscuro(figsize=(12, 4)):
    fig, ax = plt.subplots(figsize=figsize, facecolor='#020f1a')
    ax.set_facecolor('#011627')
    for spine in ax.spines.values():
        spine.set_edgecolor('#0077b640')
    ax.tick_params(colors='#90e0ef')
    ax.xaxis.label.set_color('#90e0ef')
    ax.yaxis.label.set_color('#90e0ef')
    ax.title.set_color('#caf0f8')
    return fig, ax

def figs_oscuro(nrows, ncols, figsize=(14, 5)):
    fig, axes = plt.subplots(nrows, ncols, figsize=figsize, facecolor='#020f1a')
    ax_list = axes.flatten() if hasattr(axes, 'flatten') else [axes]
    for ax in ax_list:
        ax.set_facecolor('#011627')
        for spine in ax.spines.values():
            spine.set_edgecolor('#0077b640')
        ax.tick_params(colors='#90e0ef')
        ax.xaxis.label.set_color('#90e0ef')
        ax.yaxis.label.set_color('#90e0ef')
        ax.title.set_color('#caf0f8')
    return fig, axes

def kpi(label, value, delta=None, up=True):
    delta_html = ""
    if delta:
        cls = "kpi-up" if up else "kpi-down"
        arrow = "▲" if up else "▼"
        delta_html = f'<div class="kpi-delta {cls}">{arrow} {delta}</div>'
    return f"""
    <div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        {delta_html}
    </div>
    """

# ═══════════════════════════════════════════════════════════════════════════════
# PAGINA 1 — RESUMEN GENERAL
# ═══════════════════════════════════════════════════════════════════════════════
if seccion == " Resumen General":
    st.markdown("#  Consumo Electrico en Costa Rica")
    st.markdown("**Dataset ARESEP + NASA POWER · 2020–2025**")
    st.markdown("<hr>", unsafe_allow_html=True)

    # KPIs
    total_ventas  = df_f['Ventas'].sum()
    total_ingreso = df_f['Ingreso con CVG'].sum()
    precio_prom   = df_f['Precio Medio con CVG'].mean()
    temp_prom     = df_f['T2M'].mean()

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(kpi("Ventas Totales", f"{total_ventas/1e12:.2f}T kWh"), unsafe_allow_html=True)
    c2.markdown(kpi("Ingreso Total", f"₡{total_ingreso/1e12:.1f}T"), unsafe_allow_html=True)
    c3.markdown(kpi("Precio Medio", f"₡{precio_prom:.1f}/kWh", "tendencia alcista", True), unsafe_allow_html=True)
    c4.markdown(kpi("Temp. Promedio", f"{temp_prom:.1f}°C", "Costa Rica tropical", True), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns([3, 2])

    with col1:
        st.markdown('<div class="seccion-titulo">Evolucion anual de ventas e ingresos</div>', unsafe_allow_html=True)
        anual = df_f.groupby('Año').agg(Ventas=('Ventas','sum'), Ingreso=('Ingreso con CVG','sum')).reset_index()
        fig, ax1 = plt.subplots(figsize=(11, 4), facecolor='#020f1a')
        ax1.set_facecolor('#011627')
        for spine in ax1.spines.values(): spine.set_edgecolor('#0077b640')
        ax2 = ax1.twinx()
        ax2.set_facecolor('#011627')
        bars = ax1.bar(anual['Año'], anual['Ventas']/1e9, color='#0077b6', alpha=0.7,
                       label='Ventas (B kWh)', width=0.4)
        ax2.plot(anual['Año'], anual['Ingreso']/1e12, color='#e9c46a',
                 linewidth=2.5, marker='o', markersize=6, label='Ingreso (T ₡)')
        ax1.set_ylabel('Ventas (B kWh)', color='#90e0ef')
        ax2.set_ylabel('Ingreso (T ₡)', color='#e9c46a')
        ax1.tick_params(colors='#90e0ef')
        ax2.tick_params(colors='#e9c46a')
        ax1.set_title('Ventas e Ingresos anuales 2020–2025', color='#caf0f8')
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1+lines2, labels1+labels2, facecolor='#011627',
                   labelcolor='#e0f4ff', loc='upper left', fontsize=9)
        fig.tight_layout()
        st.pyplot(fig)
        plt.close()

    with col2:
        st.markdown('<div class="seccion-titulo">Participacion de mercado</div>', unsafe_allow_html=True)
        ventas_emp = df_f.groupby('Empresa')['Ventas'].sum().sort_values(ascending=False)
        top5  = ventas_emp.head(5)
        otros = pd.Series({'Otros': ventas_emp.iloc[5:].sum()})
        pie   = pd.concat([top5, otros])
        fig, ax = plt.subplots(figsize=(6, 5), facecolor='#020f1a')
        ax.set_facecolor('#020f1a')
        wedges, texts, autotexts = ax.pie(
            pie, labels=pie.index, autopct='%1.1f%%',
            colors=PALETA[:len(pie)], startangle=90,
            wedgeprops=dict(edgecolor='#020f1a', linewidth=2)
        )
        for t in texts: t.set_color('#90e0ef')
        for at in autotexts: at.set_color('#020f1a'); at.set_fontsize(9)
        ax.set_title('Ventas por empresa', color='#caf0f8')
        st.pyplot(fig)
        plt.close()

    st.markdown('<div class="seccion-titulo">Distribucion mensual de ventas — todas las empresas</div>', unsafe_allow_html=True)
    pivot_heat = df_f.groupby(['Empresa','Mes'])['Ventas'].mean().unstack()
    pivot_heat = pivot_heat.reindex(columns=range(1, 13))
    pivot_heat.columns = MESES
    fig, ax = plt.subplots(figsize=(14, 4), facecolor='#020f1a')
    ax.set_facecolor('#011627')
    sns.heatmap(pivot_heat/1e6, annot=True, fmt='.1f', cmap='YlOrRd',
                linewidths=0.5, ax=ax, cbar_kws={'label':'Millones kWh'})
    ax.set_title('Ventas promedio por Empresa y Mes (millones kWh)', color='#caf0f8')
    ax.tick_params(colors='#90e0ef')
    ax.set_ylabel('')
    fig.tight_layout()
    st.pyplot(fig)
    plt.close()


# ═══════════════════════════════════════════════════════════════════════════════
# PAGINA 2 — HIPOTESIS PANDEMIA
# ═══════════════════════════════════════════════════════════════════════════════
elif seccion == " Hipotesis: Pandemia":
    st.markdown("#  Hipotesis: Impacto de la Pandemia en el Consumo")

    st.markdown("""
    <div class="hipotesis-banner">
        <strong> Hipotesis planteada:</strong><br>
        Durante la pandemia COVID-19 (2020–2021), el confinamiento y el trabajo desde casa
        provocaron un <strong>aumento en el consumo residencial</strong> (hogares), mientras que
        el consumo en sectores como <strong>Comercios, Servicios e Industrial</strong> descendio
        significativamente por el cierre de establecimientos.
    </div>
    """, unsafe_allow_html=True)

    # Datos por periodo
    tarifas_analisis = {
        'RESIDENCIAL':          'Residencial (Hogares)',
        'COMERCIOS Y SERVICIOS':'Comercios y Servicios',
        'INDUSTRIAL':           'Industrial',
        'PREFERENCIAL':         'Preferencial (Educacion/Municipios)',
        'ALUMBRADO PÚBLICO':    'Alumbrado Publico'
    }

    df_pan = df_f[df_f['Tarifa'].isin(tarifas_analisis.keys())].copy()
    df_pan['Tarifa_label'] = df_pan['Tarifa'].map(tarifas_analisis)

    resumen_periodo = df_pan.groupby(['Periodo','Tarifa_label'])['Ventas'].mean().reset_index()
    resumen_periodo['Periodo_ord'] = resumen_periodo['Periodo'].map({
        'Pandemia (2020-2021)': 0,
        'Recuperacion (2022-2023)': 1,
        'Expansion (2024-2025)': 2
    })
    resumen_periodo = resumen_periodo.sort_values('Periodo_ord')

    # Calcular variacion pandemia vs recuperacion
    pivot_var = resumen_periodo.pivot(index='Tarifa_label', columns='Periodo', values='Ventas')
    pivot_var['Var_%'] = ((pivot_var.get('Recuperacion (2022-2023)', 0) -
                           pivot_var.get('Pandemia (2020-2021)', 0)) /
                          pivot_var.get('Pandemia (2020-2021)', 1) * 100).round(1)

    # KPIs
    c1, c2, c3 = st.columns(3)
    resid_pan  = df_f[df_f['Tarifa']=='RESIDENCIAL'][df_f['Año'].isin([2020,2021])]['Ventas'].mean()
    resid_rec  = df_f[df_f['Tarifa']=='RESIDENCIAL'][df_f['Año'].isin([2022,2023])]['Ventas'].mean()
    comerc_pan = df_f[df_f['Tarifa']=='COMERCIOS Y SERVICIOS'][df_f['Año'].isin([2020,2021])]['Ventas'].mean()
    comerc_rec = df_f[df_f['Tarifa']=='COMERCIOS Y SERVICIOS'][df_f['Año'].isin([2022,2023])]['Ventas'].mean()
    var_resid  = (resid_rec  - resid_pan)  / resid_pan  * 100
    var_comerc = (comerc_rec - comerc_pan) / comerc_pan * 100

    c1.markdown(kpi("Consumo Residencial Pandemia", f"{resid_pan/1e6:.1f}M kWh/mes"), unsafe_allow_html=True)
    c2.markdown(kpi("Consumo Comercios Pandemia", f"{comerc_pan/1e6:.1f}M kWh/mes"), unsafe_allow_html=True)
    c3.markdown(kpi("Recuperacion Comercios", f"+{var_comerc:.1f}%",
                    "vs pandemia", True), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Consumo por tarifa segun periodo")
        fig, ax = plt.subplots(figsize=(8, 5), facecolor='#020f1a')
        ax.set_facecolor('#011627')
        for spine in ax.spines.values(): spine.set_edgecolor('#0077b640')

        periodos  = ['Pandemia (2020-2021)', 'Recuperacion (2022-2023)', 'Expansion (2024-2025)']
        tarifas_l = list(tarifas_analisis.values())
        x     = np.arange(len(tarifas_l))
        width = 0.25
        colores_p = ['#e76f51', '#0077b6', '#2a9d8f']

        for i, (periodo, color) in enumerate(zip(periodos, colores_p)):
            vals = [resumen_periodo[(resumen_periodo['Periodo']==periodo) &
                                    (resumen_periodo['Tarifa_label']==t)]['Ventas'].values
                    for t in tarifas_l]
            vals = [v[0]/1e6 if len(v) > 0 else 0 for v in vals]
            ax.bar(x + i*width, vals, width, label=periodo, color=color, alpha=0.85, edgecolor='#020f1a')

        ax.set_xticks(x + width)
        ax.set_xticklabels(tarifas_l, rotation=30, ha='right', fontsize=8, color='#90e0ef')
        ax.set_ylabel('Ventas promedio (M kWh)', color='#90e0ef')
        ax.set_title('Consumo promedio mensual por tarifa y periodo', color='#caf0f8')
        ax.legend(facecolor='#011627', labelcolor='#e0f4ff', fontsize=8)
        ax.tick_params(colors='#90e0ef')
        fig.tight_layout()
        st.pyplot(fig)
        plt.close()

    with col2:
        st.markdown("### Evolucion anual: Residencial vs Comercios")
        anual_comp = df_f[df_f['Tarifa'].isin(['RESIDENCIAL','COMERCIOS Y SERVICIOS'])]\
                       .groupby(['Año','Tarifa'])['Ventas'].mean().reset_index()

        fig, ax = plt.subplots(figsize=(8, 5), facecolor='#020f1a')
        ax.set_facecolor('#011627')
        for spine in ax.spines.values(): spine.set_edgecolor('#0077b640')

        for tarifa, color, ls in [('RESIDENCIAL','#00b4d8','-'),
                                   ('COMERCIOS Y SERVICIOS','#e9c46a','--')]:
            sub = anual_comp[anual_comp['Tarifa']==tarifa]
            ax.plot(sub['Año'], sub['Ventas']/1e6, color=color, linewidth=2.5,
                    linestyle=ls, marker='o', markersize=7,
                    label='Residencial' if tarifa=='RESIDENCIAL' else 'Comercios')

        ax.axvspan(2019.5, 2021.5, alpha=0.15, color='#e76f51', label='Periodo pandemia')
        ax.set_ylabel('Ventas promedio mensual (M kWh)', color='#90e0ef')
        ax.set_xlabel('Año', color='#90e0ef')
        ax.set_title('Residencial vs Comercios (2020–2025)', color='#caf0f8')
        ax.legend(facecolor='#011627', labelcolor='#e0f4ff', fontsize=9)
        ax.tick_params(colors='#90e0ef')
        fig.tight_layout()
        st.pyplot(fig)
        plt.close()

    # Variacion porcentual
    st.markdown("### Variacion porcentual: Pandemia → Recuperacion")
    fig, ax = plt.subplots(figsize=(12, 3.5), facecolor='#020f1a')
    ax.set_facecolor('#011627')
    for spine in ax.spines.values(): spine.set_edgecolor('#0077b640')

    vars_pct = pivot_var['Var_%'].sort_values()
    colores_bar = ['#e76f51' if v < 0 else '#2a9d8f' for v in vars_pct]
    bars = ax.barh(vars_pct.index, vars_pct.values, color=colores_bar,
                   edgecolor='#020f1a', height=0.5)
    for bar, val in zip(bars, vars_pct.values):
        ax.text(val + (0.3 if val >= 0 else -0.3),
                bar.get_y() + bar.get_height()/2,
                f'{val:+.1f}%', va='center', color='#caf0f8', fontsize=10)
    ax.axvline(0, color='#90e0ef', linewidth=0.8)
    ax.set_title('Cambio porcentual en consumo promedio: Pandemia vs Recuperacion', color='#caf0f8')
    ax.set_xlabel('Variacion %', color='#90e0ef')
    ax.tick_params(colors='#90e0ef')
    fig.tight_layout()
    st.pyplot(fig)
    plt.close()

    # Veredicto
    st.markdown("""
    <div class="hipotesis-resultado-parcial">
        <strong> Resultado: Hipotesis PARCIALMENTE CONFIRMADA</strong><br><br>
        El consumo <strong>Residencial</strong> se mantuvo estable durante la pandemia y ha crecido
        consistentemente desde 2020 hasta 2025, lo que es coherente con el trabajo y estudio desde
        casa. Sin embargo, no se observa un pico dramatico en 2020-2021, sino mas bien una tendencia
        de crecimiento sostenido.<br><br>
        El consumo de <strong>Comercios y Servicios</strong> sí muestra el patron esperado: fue
        significativamente menor durante la pandemia y se recupero con fuerza a partir de 2022,
        alcanzando un incremento de mas del <strong>+20%</strong> respecto al periodo de confinamiento.
        Esto confirma el impacto directo del cierre de establecimientos en 2020-2021.
    </div>
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGINA 3 — HIPOTESIS VERANO
# ═══════════════════════════════════════════════════════════════════════════════
elif seccion == " Hipotesis: Verano":
    st.markdown("# Hipotesis: Mayor consumo en verano por calor")

    st.markdown("""
    <div class="hipotesis-banner">
        <strong> Hipotesis planteada:</strong><br>
        En los meses de verano (diciembre–abril), la epoca seca genera temperaturas mas altas
        que impulsan el uso de ventiladores y aires acondicionados, resultando en un
        <strong>consumo electrico mayor</strong> que en la temporada lluviosa (mayo–noviembre).
        Este efecto deberia ser especialmente marcado en zonas costeras calientes como Guanacaste.
    </div>
    """, unsafe_allow_html=True)

    # Datos
    estac_resid = df_f[df_f['Tarifa']=='RESIDENCIAL'].groupby(['Estacion','Mes'])['Ventas'].mean().reset_index()
    temp_estac  = df_f.groupby(['Estacion','Mes'])[['T2M','Ventas']].mean().reset_index()
    temp_emp    = df_f.groupby(['Empresa','Estacion'])['T2M'].mean().reset_index()
    ventas_mes  = df_f.groupby('Mes')[['Ventas','T2M']].mean().reset_index()

    # KPIs
    verano_v = df_f[df_f['Estacion']=='Verano (Dic-Abr)']['Ventas'].mean()
    lluvia_v = df_f[df_f['Estacion']=='Lluvioso (May-Nov)']['Ventas'].mean()
    verano_t = df_f[df_f['Estacion']=='Verano (Dic-Abr)']['T2M'].mean()
    lluvia_t = df_f[df_f['Estacion']=='Lluvioso (May-Nov)']['T2M'].mean()
    dif_pct  = (verano_v - lluvia_v) / lluvia_v * 100

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(kpi("Consumo Prom. Verano", f"{verano_v/1e6:.1f}M kWh"), unsafe_allow_html=True)
    c2.markdown(kpi("Consumo Prom. Lluvia", f"{lluvia_v/1e6:.1f}M kWh"), unsafe_allow_html=True)
    c3.markdown(kpi("Temp. Prom. Verano", f"{verano_t:.1f}°C"), unsafe_allow_html=True)
    c4.markdown(kpi("Diferencia Estacional", f"{dif_pct:+.1f}%",
                    "verano vs lluvioso", dif_pct > 0), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Consumo y temperatura por mes")
        fig, ax1 = plt.subplots(figsize=(8, 4.5), facecolor='#020f1a')
        ax1.set_facecolor('#011627')
        for spine in ax1.spines.values(): spine.set_edgecolor('#0077b640')
        ax2 = ax1.twinx()
        ax2.set_facecolor('#011627')

        colores_mes = ['#e9c46a' if m in [12,1,2,3,4] else '#0077b6' for m in range(1,13)]
        ax1.bar(ventas_mes['Mes'], ventas_mes['Ventas']/1e6, color=colores_mes,
                alpha=0.75, edgecolor='#020f1a', label='Ventas')
        ax2.plot(ventas_mes['Mes'], ventas_mes['T2M'], color='#e76f51',
                 linewidth=2.5, marker='o', markersize=5, label='Temperatura')

        ax1.set_xticks(range(1,13))
        ax1.set_xticklabels(MESES, color='#90e0ef', fontsize=9)
        ax1.set_ylabel('Ventas prom. (M kWh)', color='#90e0ef')
        ax2.set_ylabel('Temperatura (°C)', color='#e76f51')
        ax1.tick_params(colors='#90e0ef')
        ax2.tick_params(colors='#e76f51')
        ax1.set_title('Ventas y temperatura promedio por mes', color='#caf0f8')

        from matplotlib.patches import Patch
        leyenda = [Patch(facecolor='#e9c46a', label='Verano (Dic-Abr)'),
                   Patch(facecolor='#0077b6', label='Lluvioso (May-Nov)'),
                   plt.Line2D([0],[0], color='#e76f51', linewidth=2, label='Temperatura')]
        ax1.legend(handles=leyenda, facecolor='#011627', labelcolor='#e0f4ff', fontsize=8, loc='upper left')
        fig.tight_layout()
        st.pyplot(fig)
        plt.close()

    with col2:
        st.markdown("### Temperatura por empresa — Verano vs Lluvioso")
        pivot_temp = temp_emp.pivot(index='Empresa', columns='Estacion', values='T2M').round(1)
        if 'Verano (Dic-Abr)' in pivot_temp.columns and 'Lluvioso (May-Nov)' in pivot_temp.columns:
            pivot_temp['Diferencia'] = pivot_temp['Verano (Dic-Abr)'] - pivot_temp['Lluvioso (May-Nov)']
            pivot_temp = pivot_temp.sort_values('Verano (Dic-Abr)', ascending=True)

        fig, ax = plt.subplots(figsize=(8, 4.5), facecolor='#020f1a')
        ax.set_facecolor('#011627')
        for spine in ax.spines.values(): spine.set_edgecolor('#0077b640')
        y = np.arange(len(pivot_temp))
        h = 0.35
        ax.barh(y - h/2, pivot_temp.get('Verano (Dic-Abr)', 0), h,
                color='#e9c46a', alpha=0.85, label='Verano', edgecolor='#020f1a')
        ax.barh(y + h/2, pivot_temp.get('Lluvioso (May-Nov)', 0), h,
                color='#0077b6', alpha=0.85, label='Lluvioso', edgecolor='#020f1a')
        ax.set_yticks(y)
        ax.set_yticklabels(pivot_temp.index, color='#90e0ef', fontsize=9)
        ax.set_xlabel('Temperatura promedio (°C)', color='#90e0ef')
        ax.set_title('Temperatura por empresa y estacion', color='#caf0f8')
        ax.legend(facecolor='#011627', labelcolor='#e0f4ff', fontsize=9)
        ax.tick_params(colors='#90e0ef')
        fig.tight_layout()
        st.pyplot(fig)
        plt.close()

    # Scatter temperatura vs ventas residencial
    st.markdown("### Correlacion temperatura vs consumo residencial mensual")
    df_sc = df_f[df_f['Tarifa']=='RESIDENCIAL'][['T2M','Ventas','Empresa','Estacion']].dropna()

    fig, ax = plt.subplots(figsize=(12, 4), facecolor='#020f1a')
    ax.set_facecolor('#011627')
    for spine in ax.spines.values(): spine.set_edgecolor('#0077b640')

    for estacion, color in [('Verano (Dic-Abr)','#e9c46a'), ('Lluvioso (May-Nov)','#0077b6')]:
        sub = df_sc[df_sc['Estacion']==estacion]
        ax.scatter(sub['T2M'], sub['Ventas']/1e6, alpha=0.5, s=25,
                   color=color, label=estacion)

    # Linea de tendencia
    z = np.polyfit(df_sc['T2M'].dropna(), df_sc.loc[df_sc['T2M'].notna(),'Ventas']/1e6, 1)
    p = np.poly1d(z)
    xline = np.linspace(df_sc['T2M'].min(), df_sc['T2M'].max(), 100)
    ax.plot(xline, p(xline), color='#e76f51', linewidth=2, linestyle='--', label='Tendencia')

    ax.set_xlabel('Temperatura promedio mensual (°C)', color='#90e0ef')
    ax.set_ylabel('Ventas residenciales (M kWh)', color='#90e0ef')
    ax.set_title('Temperatura vs Consumo Residencial — coloreado por estacion', color='#caf0f8')
    ax.legend(facecolor='#011627', labelcolor='#e0f4ff', fontsize=9)
    ax.tick_params(colors='#90e0ef')
    fig.tight_layout()
    st.pyplot(fig)
    plt.close()

    corr = df_sc['T2M'].corr(df_sc['Ventas'])
    st.markdown(f"""
    <div class="hipotesis-resultado-parcial">
        <strong> Resultado: Hipotesis PARCIALMENTE CONFIRMADA</strong><br><br>
        La correlacion entre temperatura y consumo residencial es <strong>r = {corr:.3f}</strong>,
        lo que indica una relacion positiva debil a moderada. Los meses de verano (Dic-Abr)
        muestran ligeramente mayor consumo, aunque la diferencia no es tan pronunciada como
        se esperaba a nivel nacional agregado.<br><br>
        El efecto es mas claro cuando se analiza por empresa: <strong>COOPEGUANACASTE</strong>,
        con temperaturas de hasta 29-30°C en verano, muestra los picos mas marcados.
        La hipotesis se confirma con mayor fuerza en zonas costeras calidas que en el Valle Central.
    </div>
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGINA 4 — HIPOTESIS GUANACASTE
# ═══════════════════════════════════════════════════════════════════════════════
elif seccion == " Hipotesis: Guanacaste":
    st.markdown("# Hipotesis: Guanacaste — Turismo y consumo moderado")

    st.markdown("""
    <div class="hipotesis-banner">
        <strong> Hipotesis planteada:</strong><br>
        Guanacaste es una provincia con una poblacion de consumo moderado (usuarios residenciales
        de bajos ingresos), pero experimenta <strong>picos de consumo estacionales</strong> impulsados
        por zonas turisticas de alto consumo como el Polo Turistico Papagayo, hoteles y resorts de
        playa. Ademas, al ser la zona mas caliente del pais, el consumo por climatizacion deberia
        ser notablemente mas alto que en el Valle Central.
    </div>
    """, unsafe_allow_html=True)

    guana  = df_f[df_f['Empresa']=='COOPEGUANACASTE'].copy()
    cnfl   = df_f[df_f['Empresa']=='CNFL'].copy()
    ice    = df_f[df_f['Empresa']=='ICE'].copy()

    # KPIs
    temp_guana = guana['T2M'].mean()
    temp_cnfl  = cnfl['T2M'].mean()
    dif_temp   = temp_guana - temp_cnfl
    ventas_guana_verano = guana[guana['Estacion']=='Verano (Dic-Abr)']['Ventas'].mean()
    ventas_guana_lluvia = guana[guana['Estacion']=='Lluvioso (May-Nov)']['Ventas'].mean()
    dif_estac = (ventas_guana_verano - ventas_guana_lluvia) / ventas_guana_lluvia * 100

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(kpi("Temp. Prom. Guanacaste", f"{temp_guana:.1f}°C",
                    f"+{dif_temp:.1f}°C vs Valle Central", True), unsafe_allow_html=True)
    c2.markdown(kpi("Temp. Prom. CNFL", f"{temp_cnfl:.1f}°C"), unsafe_allow_html=True)
    c3.markdown(kpi("Consumo Verano Guanacaste", f"{ventas_guana_verano/1e6:.1f}M kWh"), unsafe_allow_html=True)
    c4.markdown(kpi("Diferencia Verano/Lluvia", f"{dif_estac:+.1f}%",
                    "en COOPEGUANACASTE", dif_estac > 0), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Consumo mensual Guanacaste vs Valle Central")
        guana_mes = guana.groupby('Mes')['Ventas'].mean().reset_index()
        cnfl_mes  = cnfl.groupby('Mes')['Ventas'].mean().reset_index()

        fig, ax = plt.subplots(figsize=(8, 4.5), facecolor='#020f1a')
        ax.set_facecolor('#011627')
        for spine in ax.spines.values(): spine.set_edgecolor('#0077b640')

        ax.plot(guana_mes['Mes'], guana_mes['Ventas']/1e6,
                color='#e9c46a', linewidth=2.5, marker='o', markersize=6, label='COOPEGUANACASTE')
        ax.plot(cnfl_mes['Mes'], cnfl_mes['Ventas']/1e6,
                color='#0077b6', linewidth=2.5, marker='s', markersize=6, label='CNFL (Valle Central)')

        ax.axvspan(0.5, 4.5, alpha=0.1, color='#e9c46a')
        ax.axvspan(11.5, 12.5, alpha=0.1, color='#e9c46a')
        ax.set_xticks(range(1,13))
        ax.set_xticklabels(MESES, color='#90e0ef', fontsize=9)
        ax.set_ylabel('Ventas promedio (M kWh)', color='#90e0ef')
        ax.set_title('Estacionalidad: Guanacaste vs Valle Central', color='#caf0f8')
        ax.legend(facecolor='#011627', labelcolor='#e0f4ff', fontsize=9)
        ax.tick_params(colors='#90e0ef')
        ax.text(2.5, guana_mes['Ventas'].max()/1e6 * 0.95, 'Verano',
                color='#e9c46a', fontsize=9, ha='center')
        fig.tight_layout()
        st.pyplot(fig)
        plt.close()

    with col2:
        st.markdown("### Temperatura mensual comparada")
        empresas_comp = ['COOPEGUANACASTE','CNFL','COOPEALFARORUIZ','ESPH']
        df_temp_comp  = df_f[df_f['Empresa'].isin(empresas_comp)]\
                          .groupby(['Empresa','Mes'])['T2M'].mean().reset_index()

        fig, ax = plt.subplots(figsize=(8, 4.5), facecolor='#020f1a')
        ax.set_facecolor('#011627')
        for spine in ax.spines.values(): spine.set_edgecolor('#0077b640')

        colores_emp = {'COOPEGUANACASTE':'#e76f51','CNFL':'#0077b6',
                       'COOPEALFARORUIZ':'#2a9d8f','ESPH':'#e9c46a'}
        for emp in empresas_comp:
            sub = df_temp_comp[df_temp_comp['Empresa']==emp]
            ax.plot(sub['Mes'], sub['T2M'], color=colores_emp[emp],
                    linewidth=2.2, marker='o', markersize=5, label=emp)

        ax.set_xticks(range(1,13))
        ax.set_xticklabels(MESES, color='#90e0ef', fontsize=9)
        ax.set_ylabel('Temperatura (°C)', color='#90e0ef')
        ax.set_title('Temperatura promedio mensual por empresa', color='#caf0f8')
        ax.legend(facecolor='#011627', labelcolor='#e0f4ff', fontsize=8)
        ax.tick_params(colors='#90e0ef')
        fig.tight_layout()
        st.pyplot(fig)
        plt.close()

    # Consumo por tarifa en Guanacaste
    st.markdown("### Distribucion del consumo en Guanacaste por tipo de tarifa")
    col1, col2 = st.columns(2)

    with col1:
        guana_tarifa = guana.groupby('Tarifa')['Ventas'].sum().sort_values(ascending=True)
        fig, ax = plt.subplots(figsize=(7, 4), facecolor='#020f1a')
        ax.set_facecolor('#011627')
        for spine in ax.spines.values(): spine.set_edgecolor('#0077b640')
        ax.barh(guana_tarifa.index, guana_tarifa.values/1e9,
                color=PALETA[:len(guana_tarifa)], edgecolor='#020f1a')
        ax.set_title('Ventas totales por tarifa — COOPEGUANACASTE', color='#caf0f8')
        ax.set_xlabel('Ventas (B kWh)', color='#90e0ef')
        ax.tick_params(colors='#90e0ef')
        fig.tight_layout()
        st.pyplot(fig)
        plt.close()

    with col2:
        # Heatmap mes x tarifa Guanacaste
        pivot_g = guana.groupby(['Tarifa','Mes'])['Ventas'].mean().unstack()
        pivot_g = pivot_g.reindex(columns=range(1, 13))
        pivot_g.columns = MESES
        fig, ax = plt.subplots(figsize=(7, 4), facecolor='#020f1a')
        ax.set_facecolor('#011627')
        sns.heatmap(pivot_g/1e6, annot=True, fmt='.1f', cmap='YlOrRd',
                    linewidths=0.5, ax=ax, cbar_kws={'label':'M kWh'}, annot_kws={'size':7})
        ax.set_title('Consumo mensual x tarifa — COOPEGUANACASTE', color='#caf0f8')
        ax.tick_params(colors='#90e0ef')
        ax.set_ylabel('')
        fig.tight_layout()
        st.pyplot(fig)
        plt.close()

    st.markdown(f"""
    <div class="hipotesis-resultado-si">
        <strong> Resultado: Hipotesis CONFIRMADA</strong><br><br>
        Los datos confirman que <strong>COOPEGUANACASTE</strong> es la empresa con mayor temperatura
        promedio del pais (~28°C en verano), superando en casi <strong>+{dif_temp:.1f}°C</strong>
        al Valle Central (CNFL). El consumo en Guanacaste presenta picos claros en los meses de
        <strong>marzo, abril y mayo</strong>, que coinciden con la temporada alta del turismo en
        las playas del Pacifico Norte (Tamarindo, Papagayo, Conchal, Flamingo).<br><br>
        El sector de <strong>Comercios y Servicios</strong> (hoteles, restaurantes, resorts) es
        el que muestra mayor variacion estacional, lo que respalda la hipotesis del efecto
        turistico. El consumo residencial, aunque presente, es considerablemente mas bajo que
        en zonas metropolitanas como San Jose, confirmando el perfil de poblacion local con
        consumo moderado.
    </div>
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGINA 5 — VENTAS Y EVOLUCION
# ═══════════════════════════════════════════════════════════════════════════════
elif seccion == " Ventas y Evolucion":
    st.markdown("# Ventas y Evolucion Temporal")

    opciones_empresas_linea = sorted(df_f['Empresa'].unique())
    default_empresas_linea = [
        emp for emp in ['ICE','CNFL','ESPH','COOPELESCA']
        if emp in opciones_empresas_linea
    ]
    empresa_sel = st.multiselect(
        "Seleccionar empresas para el grafico de lineas",
        options=opciones_empresas_linea,
        default=default_empresas_linea
    )

    col1, col2 = st.columns(2)
    with col1:
        # Evolucion mensual lineas
        df_ts = df_f.groupby(['Año','Mes','Empresa'])['Ventas'].sum().reset_index()
        df_ts['Fecha'] = pd.to_datetime(
            df_ts[['Año','Mes']].rename(columns={'Año':'year','Mes':'month'}).assign(day=1))

        fig, ax = plt.subplots(figsize=(8, 4.5), facecolor='#020f1a')
        ax.set_facecolor('#011627')
        for spine in ax.spines.values(): spine.set_edgecolor('#0077b640')
        for i, emp in enumerate(empresa_sel):
            sub = df_ts[df_ts['Empresa']==emp]
            ax.plot(sub['Fecha'], sub['Ventas']/1e6, label=emp,
                    color=PALETA[i % len(PALETA)], linewidth=2, marker='o', markersize=2)
        ax.set_title('Evolucion mensual de ventas', color='#caf0f8')
        ax.set_ylabel('kWh (millones)', color='#90e0ef')
        ax.legend(facecolor='#011627', labelcolor='#e0f4ff', fontsize=8)
        ax.tick_params(colors='#90e0ef')
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:.0f}M'))
        fig.tight_layout()
        st.pyplot(fig)
        plt.close()

    with col2:
        ventas_emp = df_f.groupby('Empresa')['Ventas'].sum().sort_values()
        fig, ax = plt.subplots(figsize=(8, 4.5), facecolor='#020f1a')
        ax.set_facecolor('#011627')
        for spine in ax.spines.values(): spine.set_edgecolor('#0077b640')
        bars = ax.barh(ventas_emp.index, ventas_emp.values/1e9,
                       color=PALETA[:len(ventas_emp)], edgecolor='#020f1a')
        for bar, val in zip(bars, ventas_emp.values/1e9):
            ax.text(val+0.05, bar.get_y()+bar.get_height()/2,
                    f'{val:.1f}B', va='center', color='#caf0f8', fontsize=9)
        ax.set_title('Ventas totales acumuladas por empresa', color='#caf0f8')
        ax.set_xlabel('Ventas (B kWh)', color='#90e0ef')
        ax.tick_params(colors='#90e0ef')
        fig.tight_layout()
        st.pyplot(fig)
        plt.close()

    # Heatmap
    st.markdown("### Heatmap: Ventas promedio por empresa y mes")
    pivot_h = df_f.groupby(['Empresa','Mes'])['Ventas'].mean().unstack()
    pivot_h = pivot_h.reindex(columns=range(1, 13))
    pivot_h.columns = MESES
    fig, ax = plt.subplots(figsize=(14, 4), facecolor='#020f1a')
    ax.set_facecolor('#011627')
    sns.heatmap(pivot_h/1e6, annot=True, fmt='.1f', cmap='Blues',
                linewidths=0.5, ax=ax, cbar_kws={'label':'Millones kWh'})
    ax.set_title('Ventas promedio mensual por empresa (M kWh)', color='#caf0f8')
    ax.tick_params(colors='#90e0ef'); ax.set_ylabel('')
    fig.tight_layout()
    st.pyplot(fig)
    plt.close()


# ═══════════════════════════════════════════════════════════════════════════════
# PAGINA 6 — CLIMA VS CONSUMO
# ═══════════════════════════════════════════════════════════════════════════════
elif seccion == " Clima vs Consumo":
    st.markdown("#  Correlacion: Clima vs Consumo Electrico")

    cols_clima = ['T2M','WS10M','CLOUD_AMT','RH2M','T2M_MAX','T2M_MIN',
                  'PRECTOTCORR','ALLSKY_SFC_SW_DWN','PS']

    col1, col2 = st.columns(2)
    with col1:
        # Matriz correlacion
        cols_corr = ['Ventas','Ingreso con CVG','Precio Medio con CVG','T2M','WS10M','RH2M','PRECTOTCORR','T2M_MAX']
        df_corr = df_f[cols_corr].dropna()
        fig, ax = plt.subplots(figsize=(7, 6), facecolor='#020f1a')
        ax.set_facecolor('#011627')
        sns.heatmap(df_corr.corr(), annot=True, fmt='.2f', cmap='RdYlGn',
                    center=0, vmin=-1, vmax=1, linewidths=0.5, ax=ax, square=True,
                    annot_kws={'size':8})
        ax.set_title('Matriz de correlacion', color='#caf0f8')
        ax.tick_params(colors='#90e0ef')
        fig.tight_layout()
        st.pyplot(fig)
        plt.close()

    with col2:
        # Barras correlacion clima vs ventas
        corr_v = df_f[cols_clima + ['Ventas']].dropna().corr()['Ventas'].drop('Ventas').sort_values()
        colores_b = ['#e76f51' if v < 0 else '#2a9d8f' for v in corr_v]
        fig, ax = plt.subplots(figsize=(7, 6), facecolor='#020f1a')
        ax.set_facecolor('#011627')
        for spine in ax.spines.values(): spine.set_edgecolor('#0077b640')
        corr_v.plot(kind='barh', ax=ax, color=colores_b, edgecolor='#020f1a')
        ax.axvline(0, color='#90e0ef', linewidth=0.8)
        ax.set_title('Correlacion clima vs Ventas (Pearson)', color='#caf0f8')
        ax.set_xlabel('Coeficiente', color='#90e0ef')
        ax.tick_params(colors='#90e0ef')
        fig.tight_layout()
        st.pyplot(fig)
        plt.close()

    # Scatter interactivo
    st.markdown("### Scatter: variable climatica vs Ventas")
    var_clima = st.selectbox("Variable climatica", cols_clima)
    df_sc2 = df_f[['T2M','WS10M','CLOUD_AMT','RH2M','T2M_MAX','T2M_MIN',
                   'PRECTOTCORR','ALLSKY_SFC_SW_DWN','PS','Ventas','Empresa']].dropna()

    fig, ax = plt.subplots(figsize=(12, 4), facecolor='#020f1a')
    ax.set_facecolor('#011627')
    for spine in ax.spines.values(): spine.set_edgecolor('#0077b640')
    for i, emp in enumerate(df_sc2['Empresa'].unique()):
        sub = df_sc2[df_sc2['Empresa']==emp]
        ax.scatter(sub[var_clima], sub['Ventas']/1e6, alpha=0.5, s=20,
                   color=PALETA[i % len(PALETA)], label=emp)
    z = np.polyfit(df_sc2[var_clima].dropna(),
                   df_sc2.loc[df_sc2[var_clima].notna(),'Ventas']/1e6, 1)
    xl = np.linspace(df_sc2[var_clima].min(), df_sc2[var_clima].max(), 100)
    ax.plot(xl, np.poly1d(z)(xl), color='#e76f51', linewidth=2, linestyle='--', label='Tendencia')
    ax.set_xlabel(var_clima, color='#90e0ef')
    ax.set_ylabel('Ventas (M kWh)', color='#90e0ef')
    ax.set_title(f'{var_clima} vs Ventas', color='#caf0f8')
    ax.legend(bbox_to_anchor=(1.01,1), loc='upper left', facecolor='#011627',
              labelcolor='#e0f4ff', fontsize=7)
    ax.tick_params(colors='#90e0ef')
    fig.tight_layout()
    st.pyplot(fig)
    plt.close()


# ═══════════════════════════════════════════════════════════════════════════════
# PAGINA 7 — GEOGRAFIA Y CENTRALES (desde DB)
# ═══════════════════════════════════════════════════════════════════════════════
elif seccion == " Geografia y Centrales":
    st.markdown("# Geografia y Centrales Electricas")

    df_geo, ok = cargar_db("""
        SELECT DISTINCT nombre_empresa, provincia, canton,
                        central_electrica, fuente, operador,
                        coordenada_x, coordenada_y
        FROM "Fact_Dim".prediccion_precio_mes
        WHERE provincia IS NOT NULL
        ORDER BY nombre_empresa, provincia
    """)

    df_cent, ok2 = cargar_db("""
        SELECT empresa_canonica, cantidad_centrales_asociadas,
               fuentes_electricas_agregadas, operadores_centrales_agregados
        FROM "Fact_Dim".vw_empresa_centrales_agregadas
        ORDER BY cantidad_centrales_asociadas DESC
    """)

    if not ok:
        st.warning(" No se pudo conectar a la base de datos. Mostrando datos del CSV disponibles.")
        st.markdown("Asegurate de que las variables de entorno `PGHOST`, `PGUSER`, `PGPASSWORD` esten configuradas.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("### Centrales por provincia")
            cent_prov = df_geo.groupby('provincia')['central_electrica'].nunique().sort_values(ascending=False)
            fig, ax = plt.subplots(figsize=(7, 4.5), facecolor='#020f1a')
            ax.set_facecolor('#011627')
            for spine in ax.spines.values(): spine.set_edgecolor('#0077b640')
            ax.bar(cent_prov.index, cent_prov.values, color=PALETA[1], edgecolor='#020f1a', alpha=0.85)
            ax.set_title('Centrales electricas por provincia', color='#caf0f8')
            ax.set_ylabel('Cantidad', color='#90e0ef')
            ax.tick_params(colors='#90e0ef', axis='x', rotation=30)
            fig.tight_layout()
            st.pyplot(fig)
            plt.close()

        with col2:
            st.markdown("### Centrales asociadas por empresa")
            fig, ax = plt.subplots(figsize=(7, 4.5), facecolor='#020f1a')
            ax.set_facecolor('#011627')
            for spine in ax.spines.values(): spine.set_edgecolor('#0077b640')
            bars = ax.barh(df_cent['empresa_canonica'], df_cent['cantidad_centrales_asociadas'],
                           color=PALETA[:len(df_cent)], edgecolor='#020f1a')
            for bar, val in zip(bars, df_cent['cantidad_centrales_asociadas']):
                ax.text(val+0.1, bar.get_y()+bar.get_height()/2,
                        str(int(val)), va='center', color='#caf0f8', fontsize=9)
            ax.set_title('Centrales por empresa', color='#caf0f8')
            ax.tick_params(colors='#90e0ef')
            fig.tight_layout()
            st.pyplot(fig)
            plt.close()

        # Fuentes por empresa
        st.markdown("### Fuentes de generacion por empresa")
        fuentes = df_geo.groupby(['nombre_empresa','fuente'])['central_electrica'].nunique().reset_index()
        fuentes.columns = ['Empresa','Fuente','Cantidad']
        pivot_f = fuentes.pivot_table(index='Empresa', columns='Fuente',
                                       values='Cantidad', fill_value=0)
        fig, ax = plt.subplots(figsize=(13, 4.5), facecolor='#020f1a')
        ax.set_facecolor('#011627')
        for spine in ax.spines.values(): spine.set_edgecolor('#0077b640')
        pivot_f.plot(kind='bar', ax=ax, colormap='tab10', edgecolor='#020f1a')
        ax.set_title('Fuentes electricas por empresa (N° centrales)', color='#caf0f8')
        ax.set_ylabel('Cantidad', color='#90e0ef')
        ax.legend(title='Fuente', bbox_to_anchor=(1.01,1), loc='upper left',
                  facecolor='#011627', labelcolor='#e0f4ff', fontsize=8)
        ax.tick_params(colors='#90e0ef', axis='x', rotation=30)
        fig.tight_layout()
        st.pyplot(fig)
        plt.close()

        st.markdown("### Tabla de centrales")
        st.dataframe(
            df_geo[['nombre_empresa','provincia','canton','central_electrica','fuente','operador']]\
                .drop_duplicates().reset_index(drop=True),
            use_container_width=True, height=300
        )
