import streamlit as st
import pandas as pd
import altair as alt
import os

# Path to processed CSV
csv_path = os.path.join(os.path.dirname(__file__), "../data/processed/weather_clean.csv")

# Load CSV
df = pd.read_csv(csv_path)

st.title("Weather Dashboard")

# Temperature chart
st.subheader("Temperature (°C) over time")
temp_chart = alt.Chart(df).mark_line().encode(
    x='time',
    y='temperature_2m'
)
st.altair_chart(temp_chart, use_container_width=True)

# Humidity chart
st.subheader("Relative Humidity (%) over time")
hum_chart = alt.Chart(df).mark_line(color='blue').encode(
    x='time',
    y='relative_humidity_2m'
)
st.altair_chart(hum_chart, use_container_width=True)
