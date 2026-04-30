# app/pages/2_System_Trends.py
# Page 2: System-wide hourly and daily ridership trends.

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import streamlit as st
from style import apply_mta_theme, mta_header

st.set_page_config(
    page_title="System Trends · MTA Transit Pulse",
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

def style_dark_axes(ax, y_format="millions"):
    ax.set_facecolor("#0a0a0a")
    ax.tick_params(colors="white", labelsize=9)
    ax.xaxis.label.set_color("white")
    ax.yaxis.label.set_color("white")
    for spine in ax.spines.values():
        spine.set_edgecolor("#333333")
    if y_format == "millions":
        ax.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M")
        )
    elif y_format == "billions":
        ax.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: f"{x/1e9:.2f}B")
        )

# --- Cached data loaders ------------------------------------------------------

@st.cache_data
def load_hourly_system(year_filter):
    sql = f"""
        SELECT hour, SUM(ridership) AS total_ridership
        FROM ridership
        WHERE 1=1 {year_filter}
        GROUP BY hour
        ORDER BY hour ASC
    """
    return query(sql)

@st.cache_data
def load_daily_system(year_filter):
    sql = f"""
        SELECT day_of_week, SUM(ridership) AS total_ridership
        FROM ridership
        WHERE 1=1 {year_filter}
        GROUP BY day_of_week
        ORDER BY
            CASE day_of_week
                WHEN 'Monday'    THEN 1
                WHEN 'Tuesday'   THEN 2
                WHEN 'Wednesday' THEN 3
                WHEN 'Thursday'  THEN 4
                WHEN 'Friday'    THEN 5
                WHEN 'Saturday'  THEN 6
                WHEN 'Sunday'    THEN 7
            END ASC
    """
    return query(sql)

@st.cache_data
def load_borough_system(year_filter):
    sql = f"""
        SELECT borough, SUM(ridership) AS total_ridership
        FROM ridership
        WHERE 1=1 {year_filter}
        GROUP BY borough
        ORDER BY total_ridership DESC
    """
    return query(sql)

@st.cache_data
def load_top_stations_system(year_filter):
    sql = f"""
        SELECT station_complex, borough, SUM(ridership) AS total_ridership
        FROM ridership
        WHERE 1=1 {year_filter}
        GROUP BY station_complex, borough
        ORDER BY total_ridership DESC
        LIMIT 10
    """
    return query(sql)

# --- Sidebar ------------------------------------------------------------------

st.sidebar.markdown("### Filters")
year_options = ["All years", "2020", "2021", "2022", "2023", "2024"]
selected_year = st.sidebar.selectbox("Year", year_options)
year_filter = "" if selected_year == "All years" \
              else f"AND date LIKE '{selected_year}%'"

# --- Header -------------------------------------------------------------------

mta_header("📊 System Trends",
           "Network-wide ridership patterns by hour, day, and borough")

# --- Summary metrics ----------------------------------------------------------

df_hour   = load_hourly_system(year_filter)
df_day    = load_daily_system(year_filter)
df_boro   = load_borough_system(year_filter)
df_top    = load_top_stations_system(year_filter)

total_system = int(df_hour["total_ridership"].sum())
peak_hour    = int(df_hour.loc[df_hour["total_ridership"].idxmax(), "hour"])
busiest_day  = df_day.loc[df_day["total_ridership"].idxmax(), "day_of_week"]

col1, col2, col3 = st.columns(3)
col1.metric("Total System Ridership", f"{total_system:,}")
col2.metric("Peak Hour", f"{peak_hour:02d}:00")
col3.metric("Busiest Day", busiest_day)

st.markdown("---")

# --- Row 1: Hourly + Daily charts --------------------------------------------

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Ridership by Hour of Day")
    fig, ax = plt.subplots(figsize=(7, 4))
    fig.patch.set_facecolor("#0a0a0a")
    ax.plot(
        df_hour["hour"],
        df_hour["total_ridership"],
        color="#0039A6",
        linewidth=2.5,
        marker="o",
        markersize=4,
        markerfacecolor="white",
        markeredgecolor="#0039A6",
        markeredgewidth=1.5
    )
    ax.fill_between(
        df_hour["hour"],
        df_hour["total_ridership"],
        alpha=0.08,
        color="#0039A6"
    )
    # Annotate the peak hour
    peak_row = df_hour.loc[df_hour["total_ridership"].idxmax()]
    ax.annotate(
        f"Peak: {int(peak_row['hour']):02d}:00",
        xy=(peak_row["hour"], peak_row["total_ridership"]),
        xytext=(0, 14),
        textcoords="offset points",
        ha="center",
        fontsize=9,
        color="#FCCC0A"
    )
    ax.set_xlabel("Hour of Day", fontsize=10)
    ax.set_ylabel("Total Ridership", fontsize=10)
    ax.set_xticks(range(0, 24, 2))
    style_dark_axes(ax, y_format="billions")
    plt.tight_layout()
    st.pyplot(fig)
    plt.close(fig)

with col_right:
    st.subheader("Ridership by Day of Week")
    weekend = ["Saturday", "Sunday"]
    colors  = [
        "#EE352E" if d not in weekend else "#0039A6"
        for d in df_day["day_of_week"]
    ]
    fig, ax = plt.subplots(figsize=(7, 4))
    fig.patch.set_facecolor("#0a0a0a")
    ax.bar(df_day["day_of_week"], df_day["total_ridership"],
           color=colors, width=0.6)
    ax.set_xlabel("Day of Week", fontsize=10)
    ax.set_ylabel("Total Ridership", fontsize=10)
    plt.xticks(rotation=30, ha="right", color="white")
    from matplotlib.patches import Patch
    ax.legend(
        handles=[
            Patch(facecolor="#EE352E", label="Weekday"),
            Patch(facecolor="#0039A6", label="Weekend")
        ],
        facecolor="#1a1a1a", labelcolor="white", fontsize=9
    )
    style_dark_axes(ax, y_format="billions")
    plt.tight_layout()
    st.pyplot(fig)
    plt.close(fig)

st.markdown("---")

# --- Row 2: Borough + Top stations -------------------------------------------

col_left2, col_right2 = st.columns(2)

with col_left2:
    st.subheader("Ridership by Borough")
    borough_colors = {
        "Manhattan": "#EE352E",
        "Brooklyn":  "#0039A6",
        "Queens":    "#6CBE45",
        "Bronx":     "#FF6319",
        "Staten Island": "#A7A9AC"
    }
    colors_boro = [
        borough_colors.get(b, "#FCCC0A")
        for b in df_boro["borough"]
    ]
    fig, ax = plt.subplots(figsize=(7, 4))
    fig.patch.set_facecolor("#0a0a0a")
    bars = ax.barh(
        df_boro["borough"],
        df_boro["total_ridership"],
        color=colors_boro,
        height=0.5
    )
    ax.invert_yaxis()
    ax.set_xlabel("Total Ridership", fontsize=10)
    style_dark_axes(ax, y_format="millions")
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"{x/1e9:.1f}B")
    )
    ax.tick_params(axis="y", colors="white")
    plt.tight_layout()
    st.pyplot(fig)
    plt.close(fig)

with col_right2:
    st.subheader("Top 10 Stations")
    # Truncate long station names for display
    df_top["label"] = df_top["station_complex"].str[:35]
    fig, ax = plt.subplots(figsize=(7, 4))
    fig.patch.set_facecolor("#0a0a0a")
    ax.barh(
        df_top["label"],
        df_top["total_ridership"],
        color="#0039A6",
        height=0.6
    )
    ax.invert_yaxis()
    ax.set_xlabel("Total Ridership", fontsize=10)
    ax.tick_params(axis="y", colors="white", labelsize=8)
    style_dark_axes(ax, y_format="millions")
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"{x/1e9:.1f}B")
    )
    plt.tight_layout()
    st.pyplot(fig)
    plt.close(fig)

# --- Footer -------------------------------------------------------------------

st.markdown("---")
st.markdown("""
    <div style="color:#666; font-size:12px; text-align:center;">
        MTA Transit Pulse · Data: NY Open Data 2020–2024 ·
        Built with Python, SQLite & Streamlit
    </div>
""", unsafe_allow_html=True)