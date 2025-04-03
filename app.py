import streamlit as st
import pandas as pd
from typing import Optional, Dict, Any

# Import core components
from src.database_connector import DatabaseConnector
from src.profiling_engine import profile_dataframe
from src.results_manager import ResultsManager
from src.clustering_engine import ClusteringEngine
# ConfigManager and UI Components will be used later
# from src.config_manager import ConfigManager
# from src.ui_components import display_connection_form, display_results_config, ...

# --- Page Configuration ---
st.set_page_config(layout="wide", page_title="Data Profiler")
st.title("📊 Data Profiler Tool")

# --- State Management ---
# Initialize session state variables if they don't exist
if 'db_engine' not in st.session_state:
    st.session_state.db_engine = None # For source database connection
if 'results_manager' not in st.session_state:
    st.session_state.results_manager = None # For results database connection
if 'source_type' not in st.session_state:
    st.session_state.source_type = None # 'database' or 'csv'
if 'csv_df' not in st.session_state:
    st.session_state.csv_df = None # DataFrame from uploaded CSV
if 'profiled_data' not in st.session_state:
    st.session_state.profiled_data = None # List of profile dicts
if 'cluster_results' not in st.session_state:
    st.session_state.cluster_results = None # DataFrame of cluster results

# --- Helper Functions ---
def reset_state():
    """Resets connection and data state."""
    st.session_state.db_engine = None
    st.session_state.results_manager = None
    st.session_state.source_type = None
    st.session_state.csv_df = None
    st.session_state.profiled_data = None
    st.session_state.cluster_results = None
    # Potentially clear specific config inputs as well
    st.rerun()

# --- Sidebar: Configuration ---
with st.sidebar:
    st.header("⚙️ Configuration")

    # --- Source Data Connection ---
    st.subheader("1. Source Data")
    source_type = st.radio("Select Source Type", ["Database", "CSV"], key="source_choice", horizontal=True)

    conn_details_source: Optional[Dict[str, Any]] = None
    uploaded_file = None

    if source_type == "Database":
        st.session_state.source_type = "database"
        db_type_source = st.selectbox("Database Type", ["postgresql", "snowflake"], key="db_type_source")
        # TODO: Replace with dynamic form from ui_components
        if db_type_source == "postgresql":
            conn_details_source = {
                "db_type": "postgresql",
                "host": st.text_input("Host", key="pg_host"),
                "port": st.number_input("Port", value=5432, key="pg_port"),
                "username": st.text_input("Username", key="pg_user"),
                "password": st.text_input("Password", type="password", key="pg_pass"),
                "database": st.text_input("Database", key="pg_db"),
            }
        elif db_type_source == "snowflake":
             conn_details_source = {
                "db_type": "snowflake",
                "account": st.text_input("Account", key="sf_account"),
                "username": st.text_input("Username", key="sf_user"),
                "password": st.text_input("Password", type="password", key="sf_pass"),
                "warehouse": st.text_input("Warehouse", key="sf_wh"),
                "database": st.text_input("Database", key="sf_db"),
                "schema": st.text_input("Schema", key="sf_schema"), # Default schema for connection
                "role": st.text_input("Role (Optional)", key="sf_role"),
            }

        if st.button("Connect to Source DB", key="connect_source"):
            if conn_details_source and all(conn_details_source.get(k) for k in conn_details_source if k not in ['db_type', 'role']): # Basic check
                with st.spinner("Connecting to source database..."):
                    engine = DatabaseConnector.create_db_engine(conn_details_source)
                    if engine:
                        st.session_state.db_engine = engine
                        st.success("Connected to Source DB!")
                    else:
                        st.error("Connection Failed. Check details and logs.")
                        st.session_state.db_engine = None
            else:
                 st.warning("Please fill in all required connection details.")

    elif source_type == "CSV":
        st.session_state.source_type = "csv"
        uploaded_file = st.file_uploader("Upload CSV File", type=["csv"])
        if uploaded_file is not None and st.button("Load CSV", key="load_csv"):
             with st.spinner("Loading CSV..."):
                 try:
                     df = DatabaseConnector.read_csv(uploaded_file)
                     if df is not None:
                         st.session_state.csv_df = df
                         st.success(f"Loaded {uploaded_file.name} ({len(df)} rows)")
                     else:
                         st.error("Failed to read CSV file.")
                         st.session_state.csv_df = None
                 except Exception as e:
                     st.error(f"Error loading CSV: {e}")
                     st.session_state.csv_df = None

    # --- Results Database Connection ---
    st.subheader("2. Results Storage (Database)")
    # Assume results are always stored in a DB for clustering
    db_type_results = st.selectbox("Database Type", ["postgresql", "snowflake"], key="db_type_results")
    conn_details_results: Optional[Dict[str, Any]] = None
    # TODO: Replace with dynamic form from ui_components
    if db_type_results == "postgresql":
        conn_details_results = {
            "db_type": "postgresql",
            "host": st.text_input("Host", key="res_pg_host"),
            "port": st.number_input("Port", value=5432, key="res_pg_port"),
            "username": st.text_input("Username", key="res_pg_user"),
            "password": st.text_input("Password", type="password", key="res_pg_pass"),
            "database": st.text_input("Database", key="res_pg_db"),
        }
    elif db_type_results == "snowflake":
         conn_details_results = {
            "db_type": "snowflake",
            "account": st.text_input("Account", key="res_sf_account"),
            "username": st.text_input("Username", key="res_sf_user"),
            "password": st.text_input("Password", type="password", key="res_sf_pass"),
            "warehouse": st.text_input("Warehouse", key="res_sf_wh"),
            "database": st.text_input("Database", key="res_sf_db"),
            "schema": st.text_input("Schema", key="res_sf_schema"), # Schema for results table
            "role": st.text_input("Role (Optional)", key="res_sf_role"),
        }

    if st.button("Connect to Results DB", key="connect_results"):
        if conn_details_results and all(conn_details_results.get(k) for k in conn_details_results if k not in ['db_type', 'role']): # Basic check
            with st.spinner("Connecting to results database..."):
                engine = DatabaseConnector.create_db_engine(conn_details_results)
                if engine:
                    try:
                        st.session_state.results_manager = ResultsManager(engine)
                        st.success("Connected to Results DB & Manager initialized!")
                    except Exception as e:
                         st.error(f"Failed to initialize Results Manager: {e}")
                         st.session_state.results_manager = None
                else:
                    st.error("Connection Failed. Check details and logs.")
                    st.session_state.results_manager = None
        else:
             st.warning("Please fill in all required connection details.")

    # --- Reset Button ---
    st.divider()
    if st.button("Reset Connections & Data", type="secondary"):
        reset_state()


# --- Main Area ---

# --- Data Selection ---
st.header("⬇️ Select Data for Profiling")
selected_schema = None
selected_table = None
selected_columns = None
df_to_profile = None

if st.session_state.source_type == "database" and st.session_state.db_engine:
    # TODO: Implement schema/table/column selection UI
    st.info("Database connected. Implement schema/table/column selection here.")
    # Placeholder: Get schemas
    try:
        schemas = DatabaseConnector.get_schemas(st.session_state.db_engine)
        selected_schema = st.selectbox("Select Schema", schemas)
        if selected_schema:
            tables = DatabaseConnector.get_tables(st.session_state.db_engine, selected_schema)
            selected_table = st.selectbox("Select Table", tables)
            if selected_table:
                 columns_info = DatabaseConnector.get_columns(st.session_state.db_engine, selected_table, selected_schema)
                 all_columns = [c['name'] for c in columns_info]
                 selected_columns = st.multiselect("Select Columns to Profile (Default: All)", all_columns, default=all_columns)

                 # Button to load sample data for profiling
                 if st.button("Load Sample & Prepare for Profiling", key="load_sample"):
                     with st.spinner(f"Loading sample from {selected_schema}.{selected_table}..."):
                         df_sample = DatabaseConnector.get_table_sample(
                             st.session_state.db_engine,
                             selected_table,
                             selected_schema,
                             sample_size=10000 # Configurable sample size?
                         )
                         if df_sample is not None:
                             st.session_state.csv_df = df_sample # Use csv_df state to hold data
                             st.success(f"Loaded sample ({len(df_sample)} rows). Ready to profile.")
                         else:
                             st.error("Failed to load sample data.")
                             st.session_state.csv_df = None


    except Exception as e:
        st.error(f"Error interacting with source database: {e}")


elif st.session_state.source_type == "csv" and st.session_state.csv_df is not None:
    st.dataframe(st.session_state.csv_df.head())
    all_columns = st.session_state.csv_df.columns.tolist()
    selected_columns = st.multiselect("Select Columns to Profile (Default: All)", all_columns, default=all_columns)
    df_to_profile = st.session_state.csv_df # Use the loaded CSV data

# Use the sample loaded from DB if available
if st.session_state.source_type == "database" and st.session_state.csv_df is not None:
     df_to_profile = st.session_state.csv_df


# --- Profiling Execution ---
st.header("🚀 Run Profiling")
if df_to_profile is not None and selected_columns:
    if st.button("Start Profiling Selected Columns", key="start_profiling"):
        if st.session_state.results_manager:
            with st.spinner(f"Profiling {len(selected_columns)} columns..."):
                try:
                    profiles = profile_dataframe(df_to_profile, selected_columns)
                    st.session_state.profiled_data = profiles
                    st.success(f"Profiling complete for {len(profiles)} attributes.")

                    # Save results
                    with st.spinner("Saving results to database..."):
                        st.session_state.results_manager.save_profiles(profiles)
                        st.success("Results saved successfully.")

                except Exception as e:
                    st.error(f"An error occurred during profiling or saving: {e}")
                    st.session_state.profiled_data = None
        else:
            st.warning("Please connect to the Results Database first to save profiles.")
else:
    st.info("Load data (CSV or DB sample) and select columns to enable profiling.")


# --- Display Profiling Results ---
st.header("🔍 Profiling Results")
if st.session_state.profiled_data:
    # TODO: Implement better display (e.g., expandable sections per attribute)
    st.dataframe(pd.DataFrame(st.session_state.profiled_data))
else:
    st.info("Run profiling to see results here.")


# --- Clustering Execution & Results ---
st.header("🔗 Clustering Analysis")
if st.session_state.results_manager:
    # Add clustering parameters if needed (e.g., distance threshold)
    distance_threshold = st.number_input("Distance Threshold for Clustering", min_value=0.1, value=5.0, step=0.5, key="dist_thresh")

    if st.button("Run Clustering on All Stored Profiles", key="start_clustering"):
        with st.spinner("Running clustering analysis..."):
            try:
                clustering_engine = ClusteringEngine(st.session_state.results_manager)
                cluster_results_df = clustering_engine.perform_clustering(distance_threshold=distance_threshold)
                if cluster_results_df is not None:
                    st.session_state.cluster_results = cluster_results_df
                    st.success(f"Clustering complete. Found {cluster_results_df['cluster_id'].nunique()} clusters.")
                    # Display results immediately
                    st.dataframe(cluster_results_df)
                    # Optionally show updated full profiles table
                    st.subheader("Updated Profiles with Cluster IDs")
                    st.dataframe(st.session_state.results_manager.get_all_profiles())

                else:
                    st.error("Clustering failed. Check logs.")
                    st.session_state.cluster_results = None
            except Exception as e:
                st.error(f"An error occurred during clustering: {e}")
                st.session_state.cluster_results = None

    # Display previous cluster results if they exist
    elif st.session_state.cluster_results is not None:
         st.subheader("Previous Clustering Results")
         st.dataframe(st.session_state.cluster_results)

else:
    st.info("Connect to the Results Database to enable clustering.")


# --- Footer or Status Bar ---
st.divider()
st.caption("Data Profiler v0.1")

# Note: This is a basic structure. Error handling, UI refinement (using ui_components),
# state persistence, and advanced features (like async profiling) would be needed for a robust app.