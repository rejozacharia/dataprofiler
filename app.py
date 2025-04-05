import streamlit as st
import pandas as pd
from typing import Optional, Dict, Any, List, Tuple

# Import core components
from src.database_connector import DatabaseConnector
# profile_attribute is used within app_logic now
from src.results_manager import ResultsManager
from src.clustering_engine import ClusteringEngine
from src.ui_components import (
    display_db_connection_form,
    display_results_config_ui,
    display_attribute_selection,
    display_profiling_results,
    display_clustering_results,
    parse_attribute_identifier # Import the moved function
)
from src.config_manager import ConfigManager
from src.app_logic import run_profiling_job # Import the new function

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
    st.session_state.csv_filename = None # Store original filename
if 'profiled_data' not in st.session_state:
    st.session_state.profiled_data = None # List of profile dicts from last run
if 'cluster_results' not in st.session_state:
    st.session_state.cluster_results = None # DataFrame of cluster results
# NEW: List to hold attributes selected for profiling
if 'attributes_to_profile' not in st.session_state:
    st.session_state.attributes_to_profile = [] # List of strings: "schema.table.column" or "csv:filename.column"
# Removed file ID tracker as it might be unreliable

# --- Helper Functions ---
def reset_state():
    """Resets connection and data state."""
    st.session_state.db_engine = None # Keep engine/manager/data in session state
    st.session_state.results_manager = None
    st.session_state.source_type = None
    st.session_state.csv_df = None
    st.session_state.csv_filename = None # Reset filename
    st.session_state.profiled_data = None
    st.session_state.cluster_results = None
    st.session_state.attributes_to_profile = []
    config_manager.clear_all_config() # Clear config stored via manager
    # Potentially clear specific config inputs as well
    st.rerun()

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
                        # Clear potentially stale results connection details, but keep table name
                        config_manager.save_connection_details("results", None)
                        # REMOVED: config_manager.save_results_table_name("") # Ensure this line is commented/removed
                    else:
                        st.error("Connection Failed. Check details and logs.")
                        st.session_state.db_engine = None
            else:
                 st.warning("Please fill in all required connection details.")

    elif source_type == "CSV":
        st.session_state.source_type = "csv"
        uploaded_file = st.file_uploader("Upload CSV File", type=["csv"], key="csv_uploader")

        # Check if a file is uploaded AND if it's different from the one currently loaded in state
        if uploaded_file is not None:
            # Only reload and clear list if df is None OR filename changed
            if st.session_state.csv_df is None or uploaded_file.name != st.session_state.csv_filename:
                st.session_state.csv_filename = uploaded_file.name
                st.session_state.attributes_to_profile = [] # Clear list ONLY on NEW/CHANGED upload
                with st.spinner("Loading CSV..."):
                    try:
                        df = DatabaseConnector.read_csv(uploaded_file)
                        if df is not None:
                            st.session_state.csv_df = df
                            st.success(f"Loaded {uploaded_file.name} ({len(df)} rows)")
                        else:
                            st.error("Failed to read CSV file.")
                            st.session_state.csv_df = None
                            st.session_state.csv_filename = None # Reset filename on load fail
                    except Exception as e:
                        st.error(f"Error loading CSV: {e}")
                        st.session_state.csv_df = None
                        st.session_state.csv_filename = None # Reset filename on load fail
        # else: # File is the same as already loaded, do nothing to preserve state
        else:
            # If no file is present in the uploader, clear the state
            if st.session_state.csv_filename is not None: # Only clear if there *was* a file
                 st.session_state.csv_filename = None
                 st.session_state.csv_df = None
                 st.session_state.attributes_to_profile = [] # Clear list if file removed


    # --- Results Database Connection ---
    # Load source details from config manager to pass to UI component
    loaded_conn_details_source = config_manager.load_connection_details("source")
    # Use the UI component to display the results config section
    conn_details_results_form, results_table_name_form = display_results_config_ui(
        source_conn_details=loaded_conn_details_source, # Pass loaded source details
        source_is_db=(st.session_state.source_type == "database")
    )

    if st.button("Connect to Results DB", key="connect_results"):
        # Check completeness before connecting
        required_keys_results = set(conn_details_results_form.keys()) - {'db_type', 'role'} if conn_details_results_form else set()
        if conn_details_results_form and all(conn_details_results_form.get(k) for k in required_keys_results):
            with st.spinner("Connecting to results database..."):
                # Save attempted details before connecting
                config_manager.save_connection_details("results", conn_details_results_form)
                config_manager.save_results_table_name(results_table_name_form) # Save name from form
                engine = DatabaseConnector.create_db_engine(conn_details_results_form) # Use the details from the form logic
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
# Use the UI component to display attribute selection controls
display_attribute_selection()
# The component modifies st.session_state.attributes_to_profile directly

# --- Display Selected Attributes ---
st.header("📋 Attributes Selected for Profiling")
# Check if the list exists and has items
attributes_exist = 'attributes_to_profile' in st.session_state and st.session_state.attributes_to_profile
if attributes_exist:
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
            # st.rerun() # Keep commented out
else:
    st.info("Select attributes from a data source and add them to the list to enable profiling.")

# --- Profiling Execution ---
st.header("🚀 Run Profiling")
# Render the button always, but disable if no attributes are selected
profiling_disabled = not attributes_exist # Disable if list is empty or doesn't exist
if st.button("Start Profiling Listed Attributes", key="start_profiling_list", disabled=profiling_disabled):
    # Allow profiling even without results manager, but warn about saving
    can_save = st.session_state.results_manager is not None
    if not can_save:
         st.warning("Results Database not connected. Profiles will be displayed but not saved.")
try:
    # Capture the list state *immediately* after entering the button block
    # Use .copy() to ensure we have an independent list
    attributes_to_process = st.session_state.get('attributes_to_profile', []).copy()
 
    if not attributes_to_process: # Add extra check here
            st.warning("Attribute list is empty. Cannot start profiling.")
    else:
        successful_profiles, error_list = run_profiling_job(
            attributes_to_process, # Use the captured list
            st.session_state.results_manager # Pass manager (can be None)
        )
        # Store successful profiles in session state for display
        st.session_state.profiled_data = successful_profiles
        # Display errors collected from the job if any
        if error_list:
                with st.expander("View Profiling Errors"):
                    # Display errors more clearly
                    for err in error_list:
                        st.error(f"Error profiling `{err.get('attribute_name', 'Unknown')}`: {err.get('error', 'Unknown error')}")

        # st.rerun()
except Exception as e:
        st.error(f"An unexpected error occurred in the button click handler: {e}")


# --- Display Profiling Results ---
# Use the UI component
display_profiling_results()


# --- Clustering Execution & Results ---
# Use the UI component for display, keep button logic here
display_clustering_results()

# Clustering button logic remains here as it triggers backend processing
if st.session_state.results_manager:
    if st.button("Run Clustering on All Stored Profiles", key="start_clustering_main"):
        with st.spinner("Running clustering analysis..."):
            try:
                clustering_engine = ClusteringEngine(st.session_state.results_manager)
                # Get threshold from the input within display_clustering_results
                distance_threshold = st.session_state.get("dist_thresh", 5.0) # Get threshold from input widget state
                cluster_results_df = clustering_engine.perform_clustering(distance_threshold=distance_threshold)
                if cluster_results_df is not None:
                    st.session_state.cluster_results = cluster_results_df # Store results for display component
                    st.success(f"Clustering complete. Found {cluster_results_df['cluster_id'].nunique()} clusters.")
                    st.rerun() # Rerun to update the display component
                else:
                    st.error("Clustering failed. Check logs or profile data.")
                    st.session_state.cluster_results = None
            except Exception as e:
                st.error(f"An error occurred during clustering: {e}")
                st.session_state.cluster_results = None
# The display_clustering_results function handles showing the info message if no manager exists.


# --- Footer or Status Bar ---
st.divider()
st.caption("Data Profiler v0.8 - CSV State Fix Attempt 2")

# Note: This version implements multi-attribute selection & refactoring.
# Further improvements: Error details, async operations, config persistence.
