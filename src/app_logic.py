import streamlit as st
from typing import List, Dict, Any, Optional, Tuple # Added Tuple

# Import necessary components and functions
from src.database_connector import DatabaseConnector
from src.profiling_engine import profile_attribute
from src.results_manager import ResultsManager
# Import parse_attribute_identifier (assuming it's now in ui_components or a utils file)
# Let's assume it's accessible or redefine if needed. For now, import from ui_components.
from src.ui_components import parse_attribute_identifier


def _profile_single_listed_attribute(identifier: str) -> Optional[Dict[str, Any]]:
    """
    Fetches data and profiles a single attribute based on its identifier.
    (Moved from app.py)
    """
    parsed_id = parse_attribute_identifier(identifier)
    profile = None
    data_series = None

    # Ensure necessary state exists before proceeding
    db_engine = st.session_state.get('db_engine')
    csv_df = st.session_state.get('csv_df')
 
    if parsed_id["type"] == "db" and db_engine:
        schema = parsed_id["schema"]
        table = parsed_id["table"]
        column = parsed_id["column"]
        if table and column:
            # Fetch data ONLY for the specific column
            # TODO: Optimize by fetching one sample per table needed.
            df_sample = DatabaseConnector.get_table_sample(
                db_engine, table, schema, sample_size=10000 # Configurable?
            )
            if df_sample is not None and column in df_sample.columns:
                data_series = df_sample[column]
            else:
                raise ValueError(f"Column '{column}' not found in sample or sample failed for {schema}.{table}.")
        else:
             raise ValueError(f"Could not parse DB identifier correctly: {identifier}")

    elif parsed_id["type"] == "csv" and csv_df is not None:
        column = parsed_id["column"]
        if column and column in csv_df.columns:
            data_series = csv_df[column]
        else:
            raise ValueError(f"Column '{column}' not found in loaded CSV.")
    else:
        # Raise error if required state (db_engine/csv_df) is missing for the identifier type
        if parsed_id["type"] == "db" and not db_engine:
             raise ConnectionError("Database connection not available.")
        elif parsed_id["type"] == "csv" and csv_df is None:
             raise FileNotFoundError("CSV data not loaded.")
        else:
             raise ValueError(f"Cannot profile attribute: Invalid type or missing data/connection for {identifier}")

    if data_series is not None:
        # Use the identifier as the attribute name for the profile dict
        profile = profile_attribute(data_series, identifier)
        if profile:
             profile['attribute_name'] = identifier # Ensure identifier is the name
    return profile


def run_profiling_job(
   attributes_to_profile: List[str],
   results_manager: Optional[ResultsManager] # Allow None
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Runs the profiling process for a list of attributes.

    Args:
        attributes_to_profile: List of attribute identifiers.
        results_manager: Instance of ResultsManager to save results.

    Returns:
        A tuple containing:
        - List of successful profile dictionaries.
        - List of error dictionaries.
    """
    total_attributes = len(attributes_to_profile)
    st.info(f"Starting profiling for {total_attributes} attribute(s)...")
    progress_bar = st.progress(0)
    status_text = st.empty() # Placeholder for status updates
    profiles = []
    errors = []

    for i, identifier in enumerate(attributes_to_profile):
        status_text.text(f"Profiling attribute {i+1}/{total_attributes}: `{identifier}`")
        try:
            profile = _profile_single_listed_attribute(identifier)
            if profile:
                profiles.append(profile)
            else:
                # Handle case where profile function returns None (e.g., unsupported type)
                errors.append({"attribute_name": identifier, "error": "Profiling returned None (unsupported type?)"})
        except Exception as e:
            # Use st.error for immediate feedback in the UI during the loop
            st.error(f"Failed to get data or profile '{identifier}': {e}")
            errors.append({"attribute_name": identifier, "error": str(e)})

        # Update progress bar
        progress_bar.progress((i + 1) / total_attributes)

    status_text.text("Profiling run complete.")
    st.success(f"Profiled {len(profiles)} attributes successfully.")
    if errors:
        st.warning(f"Encountered errors for {len(errors)} attributes.")
        # Optionally display errors in an expander in the main app later
        # with st.expander("View Errors"):
        #     st.json([e['error'] for e in errors])

    # Save results (including errors) only if manager is available
    if results_manager and (profiles or errors):
        with st.spinner("Saving results to database..."):
            all_results_to_save = profiles + errors
            results_manager.save_profiles(all_results_to_save)
            st.success("Results (including errors) saved successfully.")
    elif not results_manager and (profiles or errors):
        st.info("Profiling complete. Results not saved as database is not connected.")

    return profiles, errors