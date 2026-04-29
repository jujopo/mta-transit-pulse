# app/streamlit_app.py
# Entry point — home page of the MTA Transit Pulse dashboard.

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

import streamlit as st
from style import apply_mta_theme, mta_header

st.set_page_config(
    page_title="MTA Transit Pulse",
    page_icon="🚇",
    layout="wide",
    initial_sidebar_state="expanded"
)

apply_mta_theme()

mta_header(
    "MTA Transit Pulse",
    "NYC Subway Ridership Analysis · 2020–2024"
)

st.markdown("""
    This dashboard explores **120 million rows** of NYC subway ridership data
    published by the Metropolitan Transportation Authority.

    Use the sidebar to navigate between sections:
""")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("""
        <div style="background:#1a1a1a; border:1px solid #0039A6;
                    border-radius:8px; padding:20px; text-align:center;">
            <div style="font-size:32px;">🏙️</div>
            <div style="font-weight:700; margin:8px 0;">Station Explorer</div>
            <div style="color:#A0A0A0; font-size:13px;">
                Ridership profile for any station in the system
            </div>
        </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
        <div style="background:#1a1a1a; border:1px solid #EE352E;
                    border-radius:8px; padding:20px; text-align:center;">
            <div style="font-size:32px;">📊</div>
            <div style="font-weight:700; margin:8px 0;">System Trends</div>
            <div style="color:#A0A0A0; font-size:13px;">
                Hourly and daily patterns across the network
            </div>
        </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
        <div style="background:#1a1a1a; border:1px solid #6CBE45;
                    border-radius:8px; padding:20px; text-align:center;">
            <div style="font-size:32px;">📈</div>
            <div style="font-weight:700; margin:8px 0;">Recovery Timeline</div>
            <div style="color:#A0A0A0; font-size:13px;">
                Year-over-year ridership since COVID-19
            </div>
        </div>
    """, unsafe_allow_html=True)

st.markdown("---")
st.markdown("""
    <div style="color:#666; font-size:12px; text-align:center;">
        Data source: MTA Subway Hourly Ridership 2020–2024 · 
        NY Open Data · Built with Python, SQLite, and Streamlit
    </div>
""", unsafe_allow_html=True)