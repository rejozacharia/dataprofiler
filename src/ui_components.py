import streamlit as st
from typing import Optional, Dict, Any, List, Tuple

def display_db_connection_form(
    config_key_prefix: str,
    db_types: List[str] = ["postgresql", "snowflake"],
    defaults: Optional[Dict[str, Any]] = None,
    disabled: bool = False
) -> Optional[Dict[str, Any]]:
    """
    Displays form elements for database connection details.

    Args:
        config_key_prefix: A unique prefix for streamlit widget keys (e.g., "src", "res").
        db_types: List of supported database types.
        defaults: Dictionary of default values to pre-fill the form.
        disabled: Whether the form fields should be disabled.

    Returns:
        A dictionary containing the connection details entered by the user, or None if incomplete.
    """
    if defaults is None:
        defaults = {}

    db_type = st.selectbox(
        "Database Type",
        db_types,
        key=f"{config_key_prefix}_db_type",
        index=db_types.index(defaults.get("db_type", db_types[0])), # Default to first type or provided default
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

    # Basic check for completeness (excluding optional 'role')
    required_keys = set(conn_details.keys()) - {'db_type', 'role'}
    if all(conn_details.get(k) for k in required_keys):
        return conn_details
    else:
        # Return the partially filled dict anyway, let the calling code decide validity
        return conn_details # Or return None? Returning dict allows pre-filling


def display_results_config_ui(
    source_conn_details: Optional[Dict[str, Any]],
    source_is_db: bool
) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Displays UI for configuring results database connection and table name.

    Args:
        source_conn_details: Connection details of the source DB (if applicable).
        source_is_db: Boolean indicating if the source is a database.

    Returns:
        A tuple containing:
        - Results DB connection details dictionary (or None).
        - Results table name string.
    """
    st.subheader("2. Results Storage (Database)")

    use_same_db = st.checkbox(
        "Use Same Connection as Source Data",
        key="same_as_source_cb",
        value=False,
        disabled=(not source_is_db) # Disable if source is not DB
    )

    results_disabled = use_same_db and source_is_db
    defaults = source_conn_details if results_disabled else {}

    # Input for results table name
    results_table_name = st.text_input(
        "Results Table Name",
        value=defaults.get("table_name", "data_profiler_results"), # Default value
        key="results_table_name_input",
        disabled=results_disabled # Disable if using same as source? Maybe allow changing table even if same DB. Let's keep enabled.
    )

    # Display the DB connection form using the helper
    conn_details_results = display_db_connection_form(
        config_key_prefix="res",
        db_types=["postgresql", "snowflake"],
        defaults=defaults,
        disabled=results_disabled
    )

    # Determine which details to actually use
    if results_disabled and source_conn_details:
        conn_details_to_use = source_conn_details.copy()
        # Ensure db_type matches the (potentially disabled but defaulted) selectbox value
        if conn_details_results:
             conn_details_to_use["db_type"] = conn_details_results.get("db_type", source_conn_details.get("db_type"))
    else:
        conn_details_to_use = conn_details_results

    return conn_details_to_use, results_table_name

# Placeholder for other potential UI components
# def display_attribute_selector(...)
# def display_profiling_results(...)
# def display_clustering_results(...)