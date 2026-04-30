# app/pages/3_Recovery_Timeline.py
# Page 3: Year-over-year COVID recovery and payment method trends.

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import streamlit as st
from style import apply_mta_theme, mta_header

st.set_page_config(
    page_title="Recovery Timeline · MTA Transit Pulse",
    page_icon="🚇",
    layout="wide"
)

apply_mta_theme()

# --- Database connection ------------------------------------------------------

@st.cache_resource
def get_connection():
    conn = sqlite3.connect("transit.db", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def query(sql, params=None):
    conn = get_connection()
    return pd.read_sql_query(sql, conn, params=params)

# --- Chart helper -------------------------------------------------------------

def style_dark_axes(ax):
    ax.set_facecolor("#0a0a0a")
    ax.tick_params(colors="white", labelsize=10)
    ax.xaxis.label.set_color("white")
    ax.yaxis.label.set_color("white")
    for spine in ax.spines.values():
        spine.set_edgecolor("#333333")
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"{x/1e9:.2f}B")
    )

# --- Cached data loaders ------------------------------------------------------

@st.cache_data
def load_recovery():
    return query("""
        SELECT
            strftime('%Y', transit_timestamp) AS year,
            SUM(ridership)                    AS total_ridership,
            ROUND(AVG(ridership), 2)          AS avg_hourly_ridership
        FROM ridership
        GROUP BY year
        ORDER BY year ASC
    """)

@st.cache_data
def load_payment():
    return query("""
        SELECT
            strftime('%Y', transit_timestamp) AS year,
            payment_method,
            SUM(ridership) AS total_ridership
        FROM ridership
        GROUP BY year, payment_method
        ORDER BY year ASC, payment_method ASC
    """)

@st.cache_data
def load_fare_class():
    return query("""
        SELECT
            fare_class_category,
            SUM(ridership) AS total_ridership
        FROM ridership
        GROUP BY fare_class_category
        ORDER BY total_ridership DESC
    """)

# --- Header -------------------------------------------------------------------

mta_header("📈 Recovery Timeline",
           "NYC subway ridership since COVID-19 · 2020–2024")

# --- Load data ----------------------------------------------------------------

df_recovery = load_recovery()
df_payment  = load_payment()
df_fare     = load_fare_class()

# --- Summary metrics ----------------------------------------------------------

ridership_2020 = df_recovery.loc[
    df_recovery["year"] == "2020", "total_ridership"
].values[0]

ridership_2024 = df_recovery.loc[
    df_recovery["year"] == "2024", "total_ridership"
].values[0]

pct_change = ((ridership_2024 - ridership_2020) / ridership_2020) * 100

omny_2024 = df_payment.loc[
    (df_payment["year"] == "2024") &
    (df_payment["payment_method"] == "OMNY"),
    "total_ridership"
].values

omny_share = 0
if len(omny_2024) > 0:
    total_2024 = df_payment.loc[
        df_payment["year"] == "2024", "total_ridership"
    ].sum()
    omny_share = (omny_2024[0] / total_2024) * 100

col1, col2, col3 = st.columns(3)
col1.metric("2020 Ridership (COVID year)", f"{ridership_2020/1e9:.2f}B")
col2.metric("2024 Ridership", f"{ridership_2024/1e9:.2f}B",
            delta=f"+{pct_change:.1f}% vs 2020")
col3.metric("OMNY Share in 2024", f"{omny_share:.1f}%")

st.markdown("---")

# --- Row 1: Recovery line chart ----------------------------------------------

st.subheader("Year-over-Year Ridership Recovery")

fig, ax = plt.subplots(figsize=(12, 5))
fig.patch.set_facecolor("#0a0a0a")

years     = df_recovery["year"].astype(str)
ridership = df_recovery["total_ridership"]

ax.plot(
    years, ridership,
    color="#EE352E",
    linewidth=2.5,
    marker="o",
    markersize=9,
    markerfacecolor="white",
    markeredgecolor="#EE352E",
    markeredgewidth=2.5,
    zorder=3
)
ax.fill_between(years, ridership, alpha=0.07, color="#EE352E")

# Annotate each year's value
for x, y in zip(years, ridership):
    ax.annotate(
        f"{y/1e9:.2f}B",
        xy=(x, y),
        xytext=(0, 16),
        textcoords="offset points",
        ha="center",
        fontsize=10,
        color="white",
        fontweight="bold"
    )

# Shade 2020 as the COVID impact year
ax.axvspan(-0.4, 0.4, alpha=0.07, color="#EE352E", label="COVID-19 impact (2020)")

ax.set_xlabel("Year", fontsize=11)
ax.set_ylabel("Total Ridership", fontsize=11)
ax.set_title("NYC Subway Ridership 2020–2024",
             fontsize=13, fontweight="bold", color="white", pad=12)
ax.legend(facecolor="#1a1a1a", labelcolor="white", fontsize=10)
style_dark_axes(ax)
plt.tight_layout()
st.pyplot(fig)
plt.close(fig)

st.markdown("---")

# --- Row 2: Payment method + Fare class --------------------------------------

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("OMNY vs MetroCard by Year")

    years_unique   = sorted(df_payment["year"].unique())
    methods        = sorted(df_payment["payment_method"].unique())
    x              = np.arange(len(years_unique))
    width          = 0.35
    method_colors  = {
        "OMNY":       "#0039A6",
        "MetroCard":  "#FF6319"
    }

    fig, ax = plt.subplots(figsize=(7, 4))
    fig.patch.set_facecolor("#0a0a0a")

    for i, method in enumerate(methods):
        subset = df_payment[df_payment["payment_method"] == method]
        offset = (i - 0.5) * width
        ax.bar(
            x + offset,
            subset["total_ridership"].values,
            width,
            label=method,
            color=method_colors.get(method, "#999999")
        )

    ax.set_xticks(x)
    ax.set_xticklabels(years_unique, fontsize=10)
    ax.set_xlabel("Year", fontsize=10)
    ax.set_ylabel("Total Ridership", fontsize=10)
    ax.legend(facecolor="#1a1a1a", labelcolor="white", fontsize=9)
    style_dark_axes(ax)
    plt.tight_layout()
    st.pyplot(fig)
    plt.close(fig)

with col_right:
    st.subheader("Ridership by Fare Class (All Years)")

    # Truncate long fare class names
    df_fare["label"] = df_fare["fare_class_category"].str[:28]

    fig, ax = plt.subplots(figsize=(7, 4))
    fig.patch.set_facecolor("#0a0a0a")

    bar_colors = [
        "#0039A6", "#EE352E", "#6CBE45", "#FF6319", "#FCCC0A",
        "#B933AD", "#00933C", "#A7A9AC", "#996633", "#00ADD0"
    ]

    ax.barh(
        df_fare["label"],
        df_fare["total_ridership"],
        color=bar_colors[:len(df_fare)],
        height=0.6
    )
    ax.invert_yaxis()
    ax.set_xlabel("Total Ridership", fontsize=10)
    ax.tick_params(axis="y", colors="white", labelsize=8)
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"{x/1e9:.1f}B")
    )
    style_dark_axes(ax)
    ax.yaxis.set_major_formatter(mticker.NullFormatter())
    plt.tight_layout()
    st.pyplot(fig)
    plt.close(fig)

# --- Insight callout ----------------------------------------------------------

st.markdown("---")
st.markdown(f"""
    <div style="
        background:#1a1a1a;
        border-left: 4px solid #FCCC0A;
        padding: 16px 20px;
        border-radius: 4px;
        font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
    ">
        <span style="color:#FCCC0A; font-weight:700;">Key Finding</span>
        <span style="color:#FFFFFF; margin-left:8px;">
            NYC subway ridership grew
            <strong style="color:#6CBE45;">+{pct_change:.1f}%</strong>
            from 2020 to 2024, driven by post-COVID recovery and the
            continued rollout of OMNY contactless payments, which reached
            <strong style="color:#0039A6;">{omny_share:.1f}%</strong>
            of all rides in 2024.
        </span>
    </div>
""", unsafe_allow_html=True)

# --- Footer -------------------------------------------------------------------

st.markdown("---")
st.markdown("""
    <div style="color:#666; font-size:12px; text-align:center;">
        MTA Transit Pulse · Data: NY Open Data 2020–2024 ·
        Built with Python, SQLite & Streamlit
    </div>
""", unsafe_allow_html=True)