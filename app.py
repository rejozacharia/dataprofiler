import streamlit as st
import pandas as pd
from typing import Optional, Dict, Any, List, Tuple

# Import core components
from src.database_connector import DatabaseConnector
from src.profiling_engine import profile_attribute # Changed import
from src.results_manager import ResultsManager
from src.clustering_engine import ClusteringEngine
from src.ui_components import display_db_connection_form, display_results_config_ui
from src.config_manager import ConfigManager

# --- Page Configuration ---
st.set_page_config(layout="wide", page_title="Data Profiler")
st.title("📊 Data Profiler Tool")

# --- Config and State Management ---
config_manager = ConfigManager() # Initialize once
# Initialize session state variables if they don't exist
if 'db_engine' not in st.session_state:
    st.session_state.db_engine = None # For source database connection
if 'results_manager' not in st.session_state:
    st.session_state.results_manager = None # For results database connection
if 'source_type' not in st.session_state:
    st.session_state.source_type = None # 'database' or 'csv'
if 'csv_df' not in st.session_state:
    st.session_state.csv_df = None # DataFrame from uploaded CSV
if 'csv_filename' not in st.session_state:
    st.session_state.csv_filename = "uploaded_csv" # Default name if not available
if 'profiled_data' not in st.session_state:
    st.session_state.profiled_data = None # List of profile dicts from last run
if 'cluster_results' not in st.session_state:
    st.session_state.cluster_results = None # DataFrame of cluster results
# NEW: List to hold attributes selected for profiling
if 'attributes_to_profile' not in st.session_state:
    st.session_state.attributes_to_profile = [] # List of strings: "schema.table.column" or "csv:filename.column"

# --- Helper Functions ---
def reset_state():
    """Resets connection and data state."""
    st.session_state.db_engine = None # Keep engine/manager/data in session state
    st.session_state.results_manager = None
    st.session_state.source_type = None
    st.session_state.csv_df = None
    st.session_state.csv_filename = "uploaded_csv"
    st.session_state.profiled_data = None
    st.session_state.cluster_results = None
    st.session_state.attributes_to_profile = []
    config_manager.clear_all_config() # Clear config stored via manager
    # Potentially clear specific config inputs as well
    st.rerun()

def format_attribute_identifier(schema: Optional[str], table: str, column: str) -> str:
    """Formats a database attribute identifier."""
    # Use a consistent separator like '::' to avoid issues with names containing '.'
    schema_part = schema if schema else "__noschema__" # Placeholder if schema is None/empty
    return f"db::{schema_part}::{table}::{column}"

def format_csv_attribute_identifier(filename: str, column: str) -> str:
    """Formats a CSV attribute identifier."""
    # Use a consistent separator
    return f"csv::{filename}::{column}"

def parse_attribute_identifier(identifier: str) -> Dict[str, Optional[str]]:
    """Parses an identifier string back into components."""
    parts = identifier.split('::')
    if len(parts) == 3 and parts[0] == 'csv':
        return {"type": "csv", "filename": parts[1], "column": parts[2]}
    elif len(parts) == 4 and parts[0] == 'db':
        schema = parts[1] if parts[1] != "__noschema__" else None
        return {"type": "db", "schema": schema, "table": parts[2], "column": parts[3]}
    else:
         st.warning(f"Could not parse identifier: {identifier}")
         return {"type": "unknown"}


# --- Sidebar: Configuration ---
with st.sidebar:
    st.header("⚙️ Configuration")

    # --- Source Data Connection ---
    st.subheader("1. Source Data")
    source_type = st.radio("Select Source Type", ["Database", "CSV"], key="source_choice", horizontal=True, on_change=lambda: st.session_state.update(attributes_to_profile=[])) # Reset list on source type change

    conn_details_source: Optional[Dict[str, Any]] = None
    uploaded_file = None

    if source_type == "Database":
        st.session_state.source_type = "database"
        db_type_source = st.selectbox("Database Type", ["postgresql", "snowflake"], key="db_type_source")
        # Use the UI component to display the form
        # Load defaults from config manager
        conn_defaults_source = config_manager.load_connection_details("source")
        conn_details_source = display_db_connection_form(
            config_key_prefix="src",
            defaults=conn_defaults_source
        )

        if st.button("Connect to Source DB", key="connect_source"):
            # Reset attribute list on new connection attempt
            st.session_state.attributes_to_profile = []
            # Check completeness within the app logic before connecting
            required_keys_source = set(conn_details_source.keys()) - {'db_type', 'role'} if conn_details_source else set()
            if conn_details_source and all(conn_details_source.get(k) for k in required_keys_source):
                with st.spinner("Connecting to source database..."):
                    # Save attempted details before connecting
                    config_manager.save_connection_details("source", conn_details_source)
                    engine = DatabaseConnector.create_db_engine(conn_details_source)
                    if engine:
                        st.session_state.db_engine = engine
                        st.success("Connected to Source DB!")
                        # Clear potentially stale results config if source changes
                        config_manager.save_connection_details("results", None)
                        config_manager.save_results_table_name("") # Use default next time by saving empty string
                    else:
                        st.error("Connection Failed. Check details and logs.")
                        st.session_state.db_engine = None
            else:
                 st.warning("Please fill in all required connection details.")

    elif source_type == "CSV":
        st.session_state.source_type = "csv"
        uploaded_file = st.file_uploader("Upload CSV File", type=["csv"])
        if uploaded_file is not None:
             # Store filename, reset attribute list
             st.session_state.csv_filename = uploaded_file.name
             st.session_state.attributes_to_profile = []
             with st.spinner("Loading CSV..."):
                 try:
                     # Read only headers first? Or load full? Load full for now.
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
    
    # Add checkbox to copy source DB details (only if source is DB)
    use_same_db = st.checkbox(
        "Use Same Connection as Source Data",
        key="same_as_source_cb",
        value=False,
        disabled=(st.session_state.source_type != "database") # Disable if source is CSV
    )
    
    # Determine if results fields should be disabled - DEFINE BEFORE USE
    results_disabled = use_same_db and st.session_state.source_type == "database"
    
    # Get source DB type if using same connection
    source_db_type = conn_details_source.get("db_type") if conn_details_source else None
    
    # Set default index based on source if checkbox is checked, otherwise default to 0 (postgresql)
    default_index = ["postgresql", "snowflake"].index(source_db_type) if results_disabled and source_db_type in ["postgresql", "snowflake"] else 0
    
    db_type_results = st.selectbox(
        "Database Type",
        ["postgresql", "snowflake"],
        key="db_type_results",
        index=default_index, # Pre-select based on source if checked
        disabled=results_disabled
    )
    # Input for results table name
    results_table_name = st.text_input(
        "Results Table Name",
        value="data_profiler_results", # Default value
        key="results_table_name_input",
        disabled=results_disabled # Disable if using same as source
    )
    
    conn_details_results: Optional[Dict[str, Any]] = None
    # TODO: Replace with dynamic form from ui_components
    if db_type_results == "postgresql":
        conn_details_results = {
            "db_type": db_type_results, # Use the selected type
            "host": st.text_input("Host", key="res_pg_host", value=conn_details_source.get("host", "") if results_disabled else "", disabled=results_disabled),
            "port": st.number_input("Port", value=conn_details_source.get("port", 5432) if results_disabled else 5432, key="res_pg_port", disabled=results_disabled),
            "username": st.text_input("Username", key="res_pg_user", value=conn_details_source.get("username", "") if results_disabled else "", disabled=results_disabled),
            "password": st.text_input("Password", type="password", key="res_pg_pass", value=conn_details_source.get("password", "") if results_disabled else "", disabled=results_disabled),
            "database": st.text_input("Database", key="res_pg_db", value=conn_details_source.get("database", "") if results_disabled else "", disabled=results_disabled),
        }
    elif db_type_results == "snowflake":
         conn_details_results = {
            "db_type": db_type_results, # Use the selected type
            "account": st.text_input("Account", key="res_sf_account", value=conn_details_source.get("account", "") if results_disabled else "", disabled=results_disabled),
            "username": st.text_input("Username", key="res_sf_user", value=conn_details_source.get("username", "") if results_disabled else "", disabled=results_disabled),
            "password": st.text_input("Password", type="password", key="res_sf_pass", value=conn_details_source.get("password", "") if results_disabled else "", disabled=results_disabled),
            "warehouse": st.text_input("Warehouse", key="res_sf_wh", value=conn_details_source.get("warehouse", "") if results_disabled else "", disabled=results_disabled),
            "database": st.text_input("Database", key="res_sf_db", value=conn_details_source.get("database", "") if results_disabled else "", disabled=results_disabled),
            "schema": st.text_input("Schema", key="res_sf_schema", value=conn_details_source.get("schema", "") if results_disabled else "", disabled=results_disabled), # Schema for results table
            "role": st.text_input("Role (Optional)", key="res_sf_role", value=conn_details_source.get("role", "") if results_disabled else "", disabled=results_disabled),
        }

    # Determine which connection details to use based on checkbox
    if results_disabled and conn_details_source:
        conn_details_to_use = conn_details_source.copy() # Use a copy to avoid modifying original
        # Ensure the db_type matches the potentially overridden selectbox if needed
        conn_details_to_use["db_type"] = db_type_results
    else:
        # Otherwise, use the values entered in the results section
        conn_details_to_use = conn_details_results
        
    if st.button("Connect to Results DB", key="connect_results"):
        # Ensure conn_details_to_use is not None before checking its contents
        # Check completeness before connecting
        required_keys_results = set(conn_details_to_use.keys()) - {'db_type', 'role'} if conn_details_to_use else set()
        if conn_details_to_use and all(conn_details_to_use.get(k) for k in required_keys_results):
            with st.spinner("Connecting to results database..."):
                # Save attempted details before connecting
                config_manager.save_connection_details("results", conn_details_to_use)
                config_manager.save_results_table_name(results_table_name) # Use correct variable name
                engine = DatabaseConnector.create_db_engine(conn_details_to_use) # Use the determined details
                if engine:
                    try:
                        # Use the saved table name when initializing
                        current_results_table_name = config_manager.load_results_table_name()
                        st.session_state.results_manager = ResultsManager(engine, table_name=current_results_table_name)
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
st.header("⬇️ Select Attributes for Profiling")
selected_schema = None
selected_table = None
selected_columns_in_table = [] # Columns currently selected in the multiselect

if st.session_state.source_type == "database" and st.session_state.db_engine:
    try:
        schemas = DatabaseConnector.get_schemas(st.session_state.db_engine)
        if schemas:
            selected_schema = st.selectbox("Select Schema", schemas, key="schema_select")
            if selected_schema:
                tables = DatabaseConnector.get_tables(st.session_state.db_engine, selected_schema)
                if tables:
                    selected_table = st.selectbox("Select Table", tables, key="table_select")
                    if selected_table:
                         columns_info = DatabaseConnector.get_columns(st.session_state.db_engine, selected_table, selected_schema)
                         all_columns = [c['name'] for c in columns_info]
                         selected_columns_in_table = st.multiselect("Select Columns", all_columns, key=f"col_select_{selected_schema}_{selected_table}")

                         if st.button("Add Columns to List", key=f"add_cols_{selected_schema}_{selected_table}"):
                             added_count = 0
                             # If no columns are selected in the multiselect, add ALL columns from the table
                             columns_to_add = all_columns if not selected_columns_in_table else selected_columns_in_table
                             
                             for col in columns_to_add:
                                 identifier = format_attribute_identifier(selected_schema, selected_table, col)
                                 if identifier not in st.session_state.attributes_to_profile:
                                     st.session_state.attributes_to_profile.append(identifier)
                                     added_count += 1
                             if added_count > 0:
                                 st.success(f"Added {added_count} attribute(s) from '{selected_table}' to the profiling list.")
                                 st.rerun() # Rerun to update the list display
                             elif columns_to_add: # Only show info if there were columns to potentially add
                                 st.info("Selected attribute(s) already in the list.")
                else:
                    st.info(f"No tables found in schema '{selected_schema}'.")
        else:
            st.warning("No schemas found or accessible.")

    except Exception as e:
        st.error(f"Error interacting with source database: {e}")


elif st.session_state.source_type == "csv" and st.session_state.csv_df is not None:
    st.dataframe(st.session_state.csv_df.head())
    all_columns = st.session_state.csv_df.columns.tolist()
    selected_columns_in_table = st.multiselect("Select Columns", all_columns, key="col_select_csv")

    if st.button("Add Columns to List", key="add_cols_csv"):
        added_count = 0
        filename = st.session_state.csv_filename
        # If no columns are selected in the multiselect, add ALL columns from the CSV
        columns_to_add = all_columns if not selected_columns_in_table else selected_columns_in_table

        for col in columns_to_add:
            identifier = format_csv_attribute_identifier(filename, col)
            if identifier not in st.session_state.attributes_to_profile:
                st.session_state.attributes_to_profile.append(identifier)
                added_count += 1
        if added_count > 0:
            st.success(f"Added {added_count} attribute(s) from '{filename}' to the profiling list.")
            st.rerun() # Rerun to update the list display
        else:
            st.info("Selected attribute(s) already in the list.")

else:
    st.info("Connect to a database or upload a CSV file to select attributes.")


# --- Display Selected Attributes & Profiling Execution ---
st.header("📋 Attributes Selected for Profiling")
if st.session_state.attributes_to_profile:
    with st.expander("View/Edit List", expanded=True):
        attributes_to_remove = []
        for i, attr_id in enumerate(st.session_state.attributes_to_profile):
            col1, col2 = st.columns([0.9, 0.1])
            col1.write(f"- `{attr_id}`")
            if col2.button("❌", key=f"remove_{i}", help="Remove attribute"):
                attributes_to_remove.append(attr_id)

        if attributes_to_remove:
            st.session_state.attributes_to_profile = [
                attr for attr in st.session_state.attributes_to_profile if attr not in attributes_to_remove
            ]
            st.rerun() # Rerun to update the displayed list immediately

    st.header("🚀 Run Profiling")
    if st.button("Start Profiling Listed Attributes", key="start_profiling_list"):
        if st.session_state.results_manager:
            total_attributes = len(st.session_state.attributes_to_profile)
            st.info(f"Starting profiling for {total_attributes} attribute(s)...")
            progress_bar = st.progress(0)
            status_text = st.empty() # Placeholder for status updates
            profiles = []
            errors = []

            for i, identifier in enumerate(st.session_state.attributes_to_profile):
                status_text.text(f"Profiling attribute {i+1}/{total_attributes}: `{identifier}`")
                parsed_id = parse_attribute_identifier(identifier)
                profile = None
                data_series = None

                try:
                    if parsed_id["type"] == "db" and st.session_state.db_engine:
                        schema = parsed_id["schema"]
                        table = parsed_id["table"]
                        column = parsed_id["column"]
                        if table and column: # Schema might be None for some DBs? Handled in format/parse
                            # Fetch data ONLY for the specific column
                            # This is inefficient as it fetches a sample for each column.
                            # TODO: Optimize by fetching one sample per table needed.
                            # For now, proceed with column-by-column fetching.
                            df_sample = DatabaseConnector.get_table_sample(
                                st.session_state.db_engine, table, schema, sample_size=10000 # Configurable?
                            )
                            if df_sample is not None and column in df_sample.columns:
                                data_series = df_sample[column]
                            else:
                                raise ValueError(f"Column '{column}' not found in sample or sample failed for {schema}.{table}.")
                        else:
                             raise ValueError(f"Could not parse DB identifier correctly: {identifier}")

                    elif parsed_id["type"] == "csv" and st.session_state.csv_df is not None:
                        column = parsed_id["column"]
                        if column and column in st.session_state.csv_df.columns:
                            data_series = st.session_state.csv_df[column]
                        else:
                            raise ValueError(f"Column '{column}' not found in loaded CSV.")
                    else:
                        raise ValueError(f"Cannot profile attribute: Invalid type or missing data/connection for {identifier}")

                    if data_series is not None:
                        # Use the identifier as the attribute name for the profile dict
                        profile = profile_attribute(data_series, identifier)

                except Exception as e:
                    st.error(f"Failed to get data or profile '{identifier}': {e}")
                    errors.append({"attribute_name": identifier, "error": str(e)}) # Use identifier as name

                if profile:
                    # Ensure the profile dict uses the unique identifier as 'attribute_name'
                    profile['attribute_name'] = identifier
                    profiles.append(profile)

                # Update progress bar
                progress_bar.progress((i + 1) / total_attributes)

            status_text.text("Profiling run complete.")
            st.session_state.profiled_data = profiles # Store successful profiles
            st.success(f"Profiled {len(profiles)} attributes successfully.")
            if errors:
                st.warning(f"Encountered errors for {len(errors)} attributes.")
                # Optionally display errors in an expander
                with st.expander("View Errors"):
                    st.json([e['error'] for e in errors])


            # Save results (including errors)
            if profiles or errors:
                with st.spinner("Saving results to database..."):
                    # Combine successful profiles and error records for saving
                    all_results_to_save = profiles + errors
                    st.session_state.results_manager.save_profiles(all_results_to_save)
                    st.success("Results (including errors) saved successfully.")

        else:
            st.warning("Please connect to the Results Database first to save profiles.")
else:
    st.info("Select attributes from a data source and add them to the list to enable profiling.")


# --- Display Profiling Results ---
st.header("🔍 Profiling Results (Last Run)")
if st.session_state.profiled_data:
    # Display only successful profiles here
    success_profiles_df = pd.DataFrame([p for p in st.session_state.profiled_data if 'error' not in p])
    if not success_profiles_df.empty:
        st.dataframe(success_profiles_df)
    else:
        st.info("No attributes were successfully profiled in the last run.")
else:
    st.info("Run profiling to see results here.")


# --- Clustering Execution & Results ---
st.header("🔗 Clustering Analysis")
if st.session_state.results_manager:
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
                    all_profiles_df = st.session_state.results_manager.get_all_profiles()
                    st.dataframe(all_profiles_df)

                else:
                    st.error("Clustering failed. Check logs or profile data.")
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
st.caption("Data Profiler v0.2")

# Note: This version implements multi-attribute selection.
# Further improvements: UI components, config management, async operations, error details.
