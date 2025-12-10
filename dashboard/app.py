import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import pathlib
from datetime import datetime
from config.database import get_engine

project_root = pathlib.Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

engine = get_engine()

st.set_page_config(page_title="Real-Time Sensor Dashboard", layout="wide")
st.title("Real-Time Sensor Dashboard")

st.sidebar.header("Settings")

auto_refresh = st.sidebar.checkbox("Enable Auto-refresh", value=False)

refresh_interval = st.sidebar.number_input("Refresh interval (seconds)", min_value=1, max_value=60, value=5)

if st.sidebar.button("Refresh Now"):
    pass

st.sidebar.subheader("Filters")
time_range = st.sidebar.selectbox("Time Range", ["Last 50 records", "Last 100 records", "Last 200 records", "Last 500 records"])

limit_map = {
    "Last 50 records": 50,
    "Last 100 records": 100,
    "Last 200 records": 200,
    "Last 500 records": 500
}
limit = limit_map[time_range]

st.sidebar.write(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")

st.subheader("Key Metrics")
metrics_col1, metrics_col2, metrics_col3 = st.columns(3)

def update_dashboard():
    query_eps = f"SELECT * FROM events_per_second ORDER BY ts DESC LIMIT {limit};"
    df_eps = pd.read_sql(query_eps, engine)
    if not df_eps.empty:
        df_eps = df_eps.sort_values("ts")
        total_events = df_eps['events_count'].sum()
        avg_events = df_eps['events_count'].mean()
        max_events = df_eps['events_count'].max()
        with metrics_col1:
            st.metric("Total Events", f"{int(total_events):,}")
        with metrics_col2:
            st.metric("Avg Events/sec", f"{avg_events:.1f}")
        with metrics_col3:
            st.metric("Peak Events/sec", f"{int(max_events)}")
        st.subheader("Events Per Second")
        fig_eps = px.line(df_eps, x="ts", y="events_count", markers=True)
        fig_eps.update_layout(xaxis_title="Timestamp", yaxis_title="Events Count", hovermode='x unified', height=400)
        st.plotly_chart(fig_eps, use_container_width=True)
    else:
        st.warning("No events data available yet.")
    st.subheader("Sensor Rolling Average")
    query_avg = f"SELECT * FROM sensor_rolling_avg ORDER BY ts DESC LIMIT {limit};"
    df_avg = pd.read_sql(query_avg, engine)
    if not df_avg.empty:
        df_avg = df_avg.sort_values("ts")
        fig_avg = px.line(df_avg, x="ts", y="rolling_avg", color="sensor_id")
        fig_avg.update_layout(xaxis_title="Timestamp", yaxis_title="Average Value", hovermode='x unified', height=400)
        st.plotly_chart(fig_avg, use_container_width=True)
    else:
        st.warning("No sensor data available yet.")

update_dashboard()

if auto_refresh:
    st.autorefresh(interval=refresh_interval * 1000, key="autorefresh")
else:
    st.info("Enable 'Auto-refresh' in the sidebar for live updates, or click 'Refresh Now' to update manually.")
