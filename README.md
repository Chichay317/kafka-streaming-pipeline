# Kafka Real-Time Streaming Pipeline

A real-time data streaming and analytics system built using **Kafka**, **Python**, **PostgreSQL**, and **Streamlit**.

## Tech Stack

- **Kafka** – message broker for streaming events
- **Python** – data ingestion and processing
- **PostgreSQL** – storage for aggregated metrics
- **Streamlit + Plotly** – live dashboard for visualization

## Features

- Consumes sensor readings in real time from a Kafka topic
- Stores events per second and sensor rolling averages in PostgreSQL
- Visualizes key metrics and trends on a live Streamlit dashboard
- Supports auto-refresh and configurable time ranges
