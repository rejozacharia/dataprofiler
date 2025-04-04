import streamlit as st
import pandas as pd
from typing import Optional, Dict, Any, List, Tuple

# Import DatabaseConnector for metadata fetching
from src.database_connector import DatabaseConnector
# Import helper functions from app.py (or move them here/to a utils file later)
# For now, assume format_attribute_identifier and format_csv_attribute_identifier are available
# or redefine them here if needed. Let's redefine for encapsulation.

def format_attribute_identifier(schema: Optional[str], table: str, column: str) -> str:
    """Formats a database attribute identifier."""
    schema_part = schema if schema else "__noschema__"
    return f"db::{schema_part}::{table}::{column}"

def format_csv_attribute_identifier(filename: str, column: str) -> str:
    """Formats a CSV attribute identifier."""
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
        # Use warning inside streamlit app context if possible, otherwise print
        try:
            st.warning(f"Could not parse identifier: {identifier}")
        except Exception:
            print(f"Warning: Could not parse identifier: {identifier}")
        return {"type": "unknown"}


def display_db_connection_form(
    config_key_prefix: str,
    db_types: List[str] = ["postgresql", "snowflake"],
    defaults: Optional[Dict[str, Any]] = None,
    disabled: bool = False
) -> Optional[Dict[str, Any]]:
    """
    Displays form elements for database connection details.
    (Code remains the same as before)
    """
    if defaults is None:
        defaults = {}

    db_type = st.selectbox(
        "Database Type",
        db_types,
        key=f"{config_key_prefix}_db_type",
        index=db_types.index(defaults.get("db_type", db_types[0])),
        disabled=disabled
    )

    conn_details: Dict[str, Any] = {"db_type": db_type}

    if db_type == "postgresql":
        conn_details.update({
            "host": st.text_input("Host", key=f"{config_key_prefix}_pg_host", value=defaults.get("host", ""), disabled=disabled),
            "port": st.number_input("Port", value=defaults.get("port", 5432), key=f"{config_key_prefix}_pg_port", disabled=disabled),
            "username": st.text_input("Username", key=f"{config_key_prefix}_pg_user", value=defaults.get("username", ""), disabled=disabled),
            "password": st.text_input("Password", type="password", key=f"{config_key_prefix}_pg_pass", value=defaults.get("password", ""), disabled=disabled),
            "database": st.text_input("Database", key=f"{config_key_prefix}_pg_db", value=defaults.get("database", ""), disabled=disabled),
        })
    elif db_type == "snowflake":
         conn_details.update({
            "account": st.text_input("Account", key=f"{config_key_prefix}_sf_account", value=defaults.get("account", ""), disabled=disabled),
            "username": st.text_input("Username", key=f"{config_key_prefix}_sf_user", value=defaults.get("username", ""), disabled=disabled),
            "password": st.text_input("Password", type="password", key=f"{config_key_prefix}_sf_pass", value=defaults.get("password", ""), disabled=disabled),
            "warehouse": st.text_input("Warehouse", key=f"{config_key_prefix}_sf_wh", value=defaults.get("warehouse", ""), disabled=disabled),
            "database": st.text_input("Database", key=f"{config_key_prefix}_sf_db", value=defaults.get("database", ""), disabled=disabled),
            "schema": st.text_input("Schema", key=f"{config_key_prefix}_sf_schema", value=defaults.get("schema", ""), disabled=disabled),
            "role": st.text_input("Role (Optional)", key=f"{config_key_prefix}_sf_role", value=defaults.get("role", ""), disabled=disabled),
        })

    return conn_details


def display_results_config_ui(
    source_conn_details: Optional[Dict[str, Any]],
    source_is_db: bool
) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Displays UI for configuring results database connection and table name.
    (Code remains the same as before)
    """
    st.subheader("2. Results Storage (Database)")

    use_same_db = st.checkbox(
        "Use Same Connection as Source Data",
        key="same_as_source_cb",
        value=False,
        disabled=(not source_is_db)
    )

    results_disabled = use_same_db and source_is_db
    defaults = source_conn_details if results_disabled else {}
    # Load saved table name using ConfigManager's method (which uses session state)
    # Need to instantiate ConfigManager here or pass it in. Let's instantiate for simplicity.
    from src.config_manager import ConfigManager # Add import inside function for encapsulation
    config_manager_ui = ConfigManager()
    saved_table_name = config_manager_ui.load_results_table_name() # Use manager method

    results_table_name = st.text_input(
        "Results Table Name",
        value=saved_table_name, # Use value loaded via ConfigManager
        key="results_table_name_input",
        # disabled=results_disabled # Keep enabled
    )

    conn_details_results = display_db_connection_form(
        config_key_prefix="res",
        db_types=["postgresql", "snowflake"],
        defaults=defaults,
        disabled=results_disabled
    )

    if results_disabled and source_conn_details:
        conn_details_to_use = source_conn_details.copy()
        if conn_details_results:
             conn_details_to_use["db_type"] = conn_details_results.get("db_type", source_conn_details.get("db_type"))
    else:
        conn_details_to_use = conn_details_results

    return conn_details_to_use, results_table_name


# --- NEW: Attribute Selection UI ---
def display_attribute_selection():
    """Displays UI for selecting attributes from DB or CSV."""
    st.header("⬇️ Select Attributes for Profiling")
    selected_schema = None
    selected_table = None
    selected_columns_in_table = []
    # removed_count initialization from here

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
                                 added_count = 0 # Initialize count for DB button click
                                 columns_to_add = all_columns if not selected_columns_in_table else selected_columns_in_table
                                 for col in columns_to_add:
                                     identifier = format_attribute_identifier(selected_schema, selected_table, col)
                                     if identifier not in st.session_state.attributes_to_profile:
                                         st.session_state.attributes_to_profile.append(identifier)
                                         added_count += 1
                                 if added_count > 0:
                                     st.success(f"Added {added_count} attribute(s) from '{selected_table}' to the profiling list.")
                                     st.rerun()
                                 elif columns_to_add:
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
            added_count = 0 # Initialize count for CSV button click
            filename = st.session_state.csv_filename
            columns_to_add = all_columns if not selected_columns_in_table else selected_columns_in_table
            for col in columns_to_add:
                identifier = format_csv_attribute_identifier(filename, col)
                if identifier not in st.session_state.attributes_to_profile:
                    st.session_state.attributes_to_profile.append(identifier)
                    added_count += 1
            if added_count > 0:
                st.success(f"Added {added_count} attribute(s) from '{filename}' to the profiling list.")
                st.rerun()
            elif columns_to_add:
                st.info("Selected attribute(s) already in the list.")
    else:
        st.info("Connect to a database or upload a CSV file to select attributes.")


# --- NEW: Profiling Results Display ---
def display_profiling_results():
    """Displays the results from the last profiling run."""
    st.header("🔍 Profiling Results (Last Run)")
    if st.session_state.profiled_data:
        # Display only successful profiles here
        success_profiles_df = pd.DataFrame([p for p in st.session_state.profiled_data if 'error' not in p])
        if not success_profiles_df.empty:
            st.dataframe(success_profiles_df)
        else:
            # Check if there were only errors
            if any('error' in p for p in st.session_state.profiled_data):
                 st.warning("Profiling ran, but all selected attributes resulted in errors.")
            else: # Should not happen if profiled_data is not None/empty, but good fallback
                 st.info("No attributes were successfully profiled in the last run.")
    else:
        st.info("Run profiling to see results here.")


# --- NEW: Clustering Results Display ---
def display_clustering_results():
    """Displays the results from the last clustering run."""
    st.header("🔗 Clustering Analysis")
    # Correct indentation for the entire block
    if st.session_state.results_manager:
        # Input for distance threshold remains here
        distance_threshold = st.number_input("Distance Threshold for Clustering", min_value=0.1, value=5.0, step=0.5, key="dist_thresh")

        # REMOVED the button definition from here - it belongs in app.py where the action is triggered

        # Display previous cluster results if they exist
        if 'cluster_results' in st.session_state and st.session_state.cluster_results is not None:
            st.subheader("Clustering Results")
            st.dataframe(st.session_state.cluster_results)
            # Optionally show updated full profiles table from DB
            with st.expander("View Full Profiles with Cluster IDs"):
                try:
                    all_profiles_df = st.session_state.results_manager.get_all_profiles()
                    st.dataframe(all_profiles_df)
                except Exception as e:
                    st.error(f"Could not retrieve full profiles: {e}")
        # else: # No previous results to show
            # st.info("Run clustering to see results.") # Covered by the outer else
    else:
        st.info("Connect to the Results Database to enable clustering.")