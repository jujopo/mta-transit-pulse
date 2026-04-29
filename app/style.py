# app/style.py
# MTA-inspired visual theme for the Streamlit dashboard.
# Import this in every page.

import streamlit as st

# MTA subway line colors — used for charts throughout the app
MTA_COLORS = {
    "A/C/E":    "#0039A6",   # blue
    "B/D/F/M":  "#FF6319",   # orange
    "G":        "#6CBE45",   # green
    "J/Z":      "#996633",   # brown
    "L":        "#A7A9AC",   # grey
    "N/Q/R/W":  "#FCCC0A",   # yellow
    "1/2/3":    "#EE352E",   # red
    "4/5/6":    "#00933C",   # dark green
    "7":        "#B933AD",   # purple
    "S":        "#808183",   # shuttle grey
}

# Chart defaults matching the MTA aesthetic
CHART_DEFAULTS = {
    "background":   "#000000",
    "text_color":   "#FFFFFF",
    "accent":       "#0039A6",
    "grid_color":   "#333333",
}

def apply_mta_theme():
    """
    Inject CSS into the Streamlit app to apply the MTA aesthetic.
    Call this at the top of every page.
    """
    st.markdown("""
        <style>
        /* Main background */
        .stApp {
            background-color: #0a0a0a;
            color: #FFFFFF;
        }

        /* Sidebar */
        [data-testid="stSidebar"] {
            background-color: #000000;
            border-right: 2px solid #0039A6;
        }

        /* Sidebar text */
        [data-testid="stSidebar"] * {
            color: #FFFFFF !important;
        }

        /* Metric cards */
        [data-testid="metric-container"] {
            background-color: #1a1a1a;
            border: 1px solid #0039A6;
            border-radius: 8px;
            padding: 12px;
        }

        /* Headers */
        h1, h2, h3 {
            font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
            letter-spacing: -0.02em;
        }

        /* Selectbox and widgets */
        [data-testid="stSelectbox"] > div {
            background-color: #1a1a1a;
            border: 1px solid #0039A6;
            color: white;
        }

        /* Divider line */
        hr {
            border-color: #0039A6;
        }
        </style>
    """, unsafe_allow_html=True)


def mta_header(title, subtitle=None):
    """
    Render a station-sign style header — white text on black
    with the MTA blue accent bar.
    """
    st.markdown(f"""
        <div style="
            background-color: #000000;
            border-left: 6px solid #0039A6;
            padding: 16px 20px;
            margin-bottom: 24px;
            font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
        ">
            <div style="font-size: 28px; font-weight: 700;
                        color: #FFFFFF; letter-spacing: -0.02em;">
                {title}
            </div>
            {"" if not subtitle else f'<div style="font-size:14px; color:#A0A0A0; margin-top:4px;">{subtitle}</div>'}
        </div>
    """, unsafe_allow_html=True)