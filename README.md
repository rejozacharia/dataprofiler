# Python Data Profiler Tool

## Overview

This project is a Python-based data profiling tool with a web interface built using Streamlit. It allows users to connect to various data sources (databases like PostgreSQL, Snowflake, and CSV files), select specific attributes (columns), and generate comprehensive profile reports.

The tool calculates various statistical metrics, identifies data types, detects common patterns (like SSN and Date of Birth), and stores these profiles in a designated database table. Additionally, it includes a feature to perform similarity analysis and clustering on the profiled attributes based on their metrics, helping users understand relationships and redundancy within their data.

## Key Features

*   **Multi-Source Connectivity:** Connect to PostgreSQL, Snowflake databases, or upload CSV files.
*   **Attribute Selection:** Interactively select schemas, tables, and specific columns for profiling across different sources.
*   **Comprehensive Profiling:** Calculates a wide range of metrics:
    *   **Common:** Record count, null count/percentage, distinct count/percentage, uniqueness.
    *   **Numeric:** Min, max, mean, median, standard deviation, variance, quantiles, histogram data.
    *   **String/Text:** Min/max/average length, top frequent values, pattern detection flags.
    *   **Date/Time:** Min/max date, time range, histogram by year.
    *   **Boolean:** True/false counts and percentage.
*   **Pattern Detection:** Identifies potential SSN and Date of Birth columns using format and logical checks.
*   **Persistent Results:** Stores profiling results in a user-configured database table (PostgreSQL or Snowflake). Handles overwriting previous profiles for the same attribute.
*   **Similarity Clustering:** Performs hierarchical clustering on all stored profiles to group similar attributes based on their metrics. Cluster IDs are stored back in the results table.
*   **Web UI:** Interactive Streamlit interface for configuration, selection, progress viewing, and results display.

## Project Structure

```
.
├── app.py                  # Main Streamlit application script
├── requirements.txt        # Project dependencies
├── PROJECT_PLAN.md         # Original architectural plan
├── LICENSE                 # Apache 2.0 License file
├── README.md               # This file
└── src/                    # Source code directory
    ├── __init__.py
    ├── app_logic.py        # Core application workflow logic (profiling job)
    ├── clustering_engine.py # Handles attribute clustering
    ├── config_manager.py   # Manages application configuration (connections, table names)
    ├── database_connector.py # Handles connections to DBs and CSV reading
    ├── pattern_detector.py # Logic for detecting SSN, DOB, etc.
    ├── profiling_engine.py # Core engine for calculating profile metrics
    ├── results_manager.py  # Handles saving/retrieving results from the database
    └── ui_components.py    # Reusable Streamlit UI elements (forms, displays)
```

## Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/rejozacharia/dataprofiler
    cd dataprofiler
    ```
2.  **Create a virtual environment (recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```
3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    *Note: Depending on your system and database choices, you might need additional system libraries or drivers (e.g., for PostgreSQL).*

## Usage

1.  **Run the Streamlit application:**
    ```bash
    streamlit run app.py
    ```
2.  **Open your web browser** to the local URL provided by Streamlit (usually `http://localhost:8501`).
3.  **Configure Connections:**
    *   Use the sidebar to select the **Source Data** type (Database or CSV).
    *   Enter the required connection details for your source database or upload a CSV file.
    *   Configure the **Results Storage** database connection details and specify the desired table name (or use the default). You can opt to use the same connection as the source if applicable.
    *   Click "Connect to Source DB" and "Connect to Results DB".
4.  **Select Attributes:**
    *   Use the selectors in the main area to choose the schema, table, and columns (for databases) or just columns (for CSV).
    *   Click "Add Columns to List" to add selected attributes (or all if none are selected) to the profiling queue.
    *   View and manage the selected attributes in the "Attributes Selected for Profiling" expander.
5.  **Run Profiling:**
    *   Click the "Start Profiling Listed Attributes" button. Progress will be shown.
    *   View the generated profiles in the "Profiling Results" section. Results (including any errors) are saved to the configured results database table.
6.  **Run Clustering:**
    *   Adjust the "Distance Threshold" if desired.
    *   Click "Run Clustering on All Stored Profiles".
    *   View the cluster assignments and the updated profiles table in the "Clustering Analysis" section.

## License

This project is licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for details.
