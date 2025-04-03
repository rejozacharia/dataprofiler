import streamlit as st
from typing import Optional, Dict, Any

# Define keys for session state storage
SRC_CONN_KEY = "config_source_connection"
RES_CONN_KEY = "config_results_connection"
RES_TABLE_KEY = "config_results_table_name"
DEFAULT_TABLE_NAME = "data_profiler_results"

class ConfigManager:
    """
    Manages application configuration using Streamlit's session state.
    (Note: This implementation does not persist config across app restarts.)
    """

    def save_connection_details(self, config_type: str, details: Optional[Dict[str, Any]]):
        """Saves connection details to session state."""
        key = SRC_CONN_KEY if config_type == "source" else RES_CONN_KEY
        st.session_state[key] = details

    def load_connection_details(self, config_type: str) -> Optional[Dict[str, Any]]:
        """Loads connection details from session state."""
        key = SRC_CONN_KEY if config_type == "source" else RES_CONN_KEY
        return st.session_state.get(key, None)

    def save_results_table_name(self, table_name: str):
        """Saves the results table name to session state."""
        st.session_state[RES_TABLE_KEY] = table_name

    def load_results_table_name(self) -> str:
        """Loads the results table name from session state."""
        return st.session_state.get(RES_TABLE_KEY, DEFAULT_TABLE_NAME)

    def clear_all_config(self):
        """Clears stored configuration from session state."""
        for key in [SRC_CONN_KEY, RES_CONN_KEY, RES_TABLE_KEY]:
            if key in st.session_state:
                del st.session_state[key]
        print("Cleared configuration from session state.")

# Example usage (not run directly, but shows intent)
# if __name__ == '__main__':
#     manager = ConfigManager()
#     # Example: Saving source details
#     src_details = {"db_type": "postgresql", "host": "localhost", ...}
#     manager.save_connection_details("source", src_details)
#     # Example: Loading results details
#     res_details = manager.load_connection_details("results")
#     # Example: Saving table name
#     manager.save_results_table_name("my_profile_results")
#     table_name = manager.load_results_table_name()