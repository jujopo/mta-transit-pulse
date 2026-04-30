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
    page_title="Station Explorer · MTA Transit Pulse",
    page_icon="🚇",
    layout="wide"
)

apply_mta_theme()

@st.cache_resource
def get_connection():
    conn = sqlite3.connect("transit.db", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def query(sql, params=None):
    conn = get_connection()
    return pd.read_sql_query(sql, conn, params=params)

# --- Cached data loaders ------------------------------------------------------

@st.cache_data
def load_stations():
    df = query("SELECT DISTINCT station_complex FROM ridership ORDER BY station_complex ASC")
    return df["station_complex"].tolist()

@st.cache_data
def load_summary(station, year_filter):
    sql = f"""
        SELECT
            SUM(ridership)           AS total_ridership,
            ROUND(AVG(ridership), 0) AS avg_hourly_ridership,
            COUNT(DISTINCT date)     AS days_in_dataset
        FROM ridership
        WHERE station_complex = ?
        {year_filter}
    """
    return query(sql, params=(station,))

@st.cache_data
def load_hourly(station, year_filter):
    sql = f"""
        SELECT hour, SUM(ridership) AS total_ridership
        FROM ridership
        WHERE station_complex = ?
        {year_filter}
        GROUP BY hour
        ORDER BY hour ASC
    """
    return query(sql, params=(station,))

@st.cache_data
def load_daily(station, year_filter):
    sql = f"""
        SELECT day_of_week, SUM(ridership) AS total_ridership
        FROM ridership
        WHERE station_complex = ?
        {year_filter}
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
    return query(sql, params=(station,))

# --- Chart helpers ------------------------------------------------------------

def style_dark_axes(ax):
    """Apply MTA dark theme to a matplotlib axes object."""
    ax.set_facecolor("#0a0a0a")
    ax.tick_params(colors="white", labelsize=9)
    ax.xaxis.label.set_color("white")
    ax.yaxis.label.set_color("white")
    for spine in ax.spines.values():
        spine.set_edgecolor("#333333")
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M" if x >= 1e6 else f"{x:,.0f}")
    )


def plot_hourly(df):
    fig, ax = plt.subplots(figsize=(7, 4))
    fig.patch.set_facecolor("#0a0a0a")
    ax.plot(
        df["hour"],
        df["total_ridership"],
        color="#0039A6",
        linewidth=2.5,
        marker="o",
        markersize=5,
        markerfacecolor="white",
        markeredgecolor="#0039A6",
        markeredgewidth=1.5
    )
    ax.fill_between(df["hour"], df["total_ridership"], alpha=0.08, color="#0039A6")
    ax.set_xlabel("Hour of Day", fontsize=10)
    ax.set_ylabel("Total Ridership", fontsize=10)
    ax.set_xticks(range(0, 24, 2))
    style_dark_axes(ax)
    plt.tight_layout()
    return fig


def plot_daily(df):
    weekend = ["Saturday", "Sunday"]
    colors = [
        "#EE352E" if day not in weekend else "#0039A6"
        for day in df["day_of_week"]
    ]
    fig, ax = plt.subplots(figsize=(7, 4))
    fig.patch.set_facecolor("#0a0a0a")
    ax.bar(df["day_of_week"], df["total_ridership"], color=colors, width=0.6)
    ax.set_xlabel("Day of Week", fontsize=10)
    ax.set_ylabel("Total Ridership", fontsize=10)
    plt.xticks(rotation=30, ha="right")

    # Manual legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor="#EE352E", label="Weekday"),
        Patch(facecolor="#0039A6", label="Weekend")
    ]
    ax.legend(handles=legend_elements, facecolor="#1a1a1a",
              labelcolor="white", fontsize=9)
    style_dark_axes(ax)
    plt.tight_layout()
    return fig

# --- Sidebar ------------------------------------------------------------------

st.sidebar.markdown("### Filters")
year_options = ["All years", "2020", "2021", "2022", "2023", "2024"]
selected_year = st.sidebar.selectbox("Year", year_options)
year_filter = "" if selected_year == "All years" \
              else f"AND date LIKE '{selected_year}%'"

# --- Main layout --------------------------------------------------------------

mta_header("🏙️ Station Explorer",
           "Select any station to explore its ridership profile")

stations = load_stations()

default_station = "Grand Central-42 St (S,4,5,6,7)"
default_index = stations.index(default_station) \
                if default_station in stations else 0

selected_station = st.selectbox(
    "Choose a station",
    options=stations,
    index=default_index
)

# --- Metric cards -------------------------------------------------------------

df_summary = load_summary(selected_station, year_filter)

total    = int(df_summary["total_ridership"].iloc[0] or 0)
avg_hour = int(df_summary["avg_hourly_ridership"].iloc[0] or 0)
days     = int(df_summary["days_in_dataset"].iloc[0] or 0)

col1, col2, col3 = st.columns(3)
col1.metric("Total Ridership", f"{total:,}")
col2.metric("Avg Riders per Hour-Slot", f"{avg_hour:,}")
col3.metric("Days of Data", f"{days:,}")

# --- Charts -------------------------------------------------------------------

st.markdown("---")
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Ridership by Hour of Day")
    df_hour = load_hourly(selected_station, year_filter)
    if not df_hour.empty:
        fig = plot_hourly(df_hour)
        st.pyplot(fig)
        plt.close(fig)
    else:
        st.info("No data available for this selection.")

with col_right:
    st.subheader("Ridership by Day of Week")
    df_day = load_daily(selected_station, year_filter)
    if not df_day.empty:
        fig = plot_daily(df_day)
        st.pyplot(fig)
        plt.close(fig)
    else:
        st.info("No data available for this selection.")

# --- Footer -------------------------------------------------------------------

st.markdown("---")
st.markdown("""
    <div style="color:#666; font-size:12px; text-align:center;">
        MTA Transit Pulse · Data: NY Open Data 2020–2024 ·
        Built with Python, SQLite & Streamlit
    </div>
""", unsafe_allow_html=True)