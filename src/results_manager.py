import pandas as pd
import numpy as np # <-- ADDED IMPORT
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.schema import Table, MetaData, Column
from sqlalchemy.types import String, Integer, Float, DateTime, Boolean, JSON, Text
from sqlalchemy.dialects.postgresql import insert as pg_insert
# Note: Snowflake upsert might require MERGE, handled differently.
# For simplicity, we might start with delete+insert or rely on specific dialects.
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Dict, Any, Optional
from datetime import datetime
import warnings
import json # For handling JSON serializable types before DB insertion

# Define the default table name
DEFAULT_RESULTS_TABLE_NAME = 'data_profiler_results'
# Define columns based on PROJECT_PLAN.md metrics
# Need to handle potential variations and ensure types are DB-compatible
# Using JSON or Text for complex types like quantiles/histograms/top_values
def get_results_table_definition(table_name: str, metadata: MetaData) -> Table:
   """Creates the SQLAlchemy Table object for the results table."""
   return Table(
       table_name, metadata,
       Column('attribute_name', String, primary_key=True), # Using unique identifier from app.py format
       Column('profile_date', DateTime, nullable=False),
       Column('cluster_id', Integer, nullable=True), # Added later by clustering engine

       # Common Metrics
       Column('data_type_detected', String),
       Column('total_records', Integer),
       Column('null_count', Integer),
       Column('null_percentage', Float),
       Column('distinct_count', Integer),
       Column('distinct_percentage', Float),
       Column('is_unique', Boolean),

       # Numeric Metrics
       Column('min', Float, nullable=True),
       Column('max', Float, nullable=True),
       Column('mean', Float, nullable=True),
       Column('median', Float, nullable=True),
       Column('std_dev', Float, nullable=True),
       Column('variance', Float, nullable=True),
       Column('quantiles', JSON, nullable=True), # Store as JSON
       Column('histogram', JSON, nullable=True), # Store as JSON

       # String/Text Metrics
       Column('min_length', Integer, nullable=True),
       Column('max_length', Integer, nullable=True),
       Column('avg_length', Float, nullable=True),
       Column('top_values', JSON, nullable=True), # Store as JSON
       Column('is_ssn_candidate', Boolean, nullable=True),
       Column('is_dob_candidate', Boolean, nullable=True),
       Column('top_1_frequency_pct', Float, nullable=True),
       Column('top_5_frequency_pct', Float, nullable=True),

       # Date/Time Metrics
       Column('min_date', String, nullable=True), # Store ISO format string
       Column('max_date', String, nullable=True), # Store ISO format string
       Column('time_range_days', Float, nullable=True),
       Column('histogram_by_year', JSON, nullable=True), # Store as JSON

       # Boolean Metrics
       Column('true_count', Integer, nullable=True),
       Column('false_count', Integer, nullable=True),
       Column('true_percentage', Float, nullable=True),

       # Error column
       Column('error', Text, nullable=True) # To store profiling errors
   )


class ResultsManager:
    """Handles saving and retrieving profiling results from a database."""

    def __init__(self, engine: Engine, table_name: str = DEFAULT_RESULTS_TABLE_NAME):
        """
        Initializes the ResultsManager with a SQLAlchemy engine and table name.

        Args:
            engine: SQLAlchemy Engine object connected to the results database.
            table_name: The name for the results table.
        """
        if not isinstance(engine, Engine):
            raise TypeError("Engine must be a valid SQLAlchemy Engine object.")
        self.engine = engine
        self.metadata = MetaData() # Each manager instance gets its own metadata
        self.table_name = table_name if table_name else DEFAULT_RESULTS_TABLE_NAME # Ensure table name is not empty
        self.results_table = get_results_table_definition(self.table_name, self.metadata)
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Creates the results table if it doesn't exist."""
        try:
            print(f"Ensuring results table '{self.table_name}' exists...")
            self.metadata.create_all(self.engine) # Use instance metadata
            print(f"Table '{self.table_name}' check complete.")
        except SQLAlchemyError as e:
            warnings.warn(f"Error checking/creating results table: {e}", UserWarning)
            # Decide if this should be a fatal error
            raise

    def _serialize_complex_types(self, profile_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Ensure complex types (like numpy types) are JSON serializable."""
        serialized_data = []
        for record in profile_data:
            new_record = {}
            for key, value in record.items():
                if isinstance(value, (np.integer, np.int64)):
                    new_record[key] = int(value)
                elif isinstance(value, (np.floating, np.float64)):
                    new_record[key] = float(value)
                elif isinstance(value, np.ndarray):
                    new_record[key] = value.tolist() # Convert numpy arrays
                elif isinstance(value, (dict, list)):
                     # Recursively check dicts/lists for numpy types? For now, assume top-level is enough
                     # Or rely on JSON encoder to handle basic types within structures
                     new_record[key] = value
                elif pd.isna(value): # Handle Pandas NA or Numpy NaN
                    new_record[key] = None
                else:
                    new_record[key] = value
            serialized_data.append(new_record)
        return serialized_data


    def save_profiles(self, profile_data: List[Dict[str, Any]]):
        """
        Saves profiling results to the database, overwriting existing entries
        for the same attribute name.

        Args:
            profile_data: A list of dictionaries, where each dict is a profile
                          for one attribute, matching the results_table schema
                          (excluding profile_date and cluster_id).
        """
        if not profile_data:
            print("No profile data provided to save.")
            return

        now = datetime.now()
        prepared_data = []
        for profile in profile_data:
            # Ensure all expected columns are present, adding None if missing
            record = {"profile_date": now}
            for col in self.results_table.columns: # Use instance table object
                if col.name not in ['profile_date', 'cluster_id']: # These are handled separately
                     record[col.name] = profile.get(col.name) # Use .get() for safety
            prepared_data.append(record)

        # Ensure data is JSON serializable before inserting
        try:
            serializable_data = self._serialize_complex_types(prepared_data)
        except Exception as e:
            warnings.warn(f"Error serializing profile data: {e}. Skipping save.", UserWarning)
            return

        dialect_name = self.engine.dialect.name

        try:
            with self.engine.begin() as connection: # Use transaction
                if dialect_name == 'postgresql':
                    # Use INSERT ... ON CONFLICT for PostgreSQL upsert
                    for record in serializable_data:
                        # Filter out None values if the dialect doesn't handle them well in JSON casts
                        # record = {k: v for k, v in record.items() if v is not None}
                        stmt = pg_insert(self.results_table).values(record) # Use instance table object
                        # Define update statement for conflict
                        update_dict = {col.name: col for col in stmt.excluded if col.name not in ['attribute_name']}
                        stmt = stmt.on_conflict_do_update(
                            index_elements=['attribute_name'], # Primary key column(s)
                            set_=update_dict
                        )
                        connection.execute(stmt)
                    print(f"Successfully upserted {len(serializable_data)} profiles using PostgreSQL ON CONFLICT.")

                # elif dialect_name == 'snowflake':
                    # Snowflake typically uses MERGE. Implementing MERGE via SQLAlchemy Core
                    # can be complex. A simpler (but less atomic) approach is delete+insert.
                    # Or use session.merge() if using ORM.
                    # Let's use delete + insert for simplicity here.
                    # print("Using DELETE+INSERT approach for Snowflake/other dialects.")
                    # attribute_names = [record['attribute_name'] for record in serializable_data]
                    # delete_stmt = self.results_table.delete().where(self.results_table.c.attribute_name.in_(attribute_names)) # Use instance table
                    # connection.execute(delete_stmt)
                    # connection.execute(self.results_table.insert(), serializable_data) # Use instance table
                    # print(f"Successfully saved {len(serializable_data)} profiles using DELETE+INSERT.")

                else: # Fallback: Delete then Insert (less efficient, not atomic)
                    warnings.warn(f"Using DELETE+INSERT fallback for dialect '{dialect_name}'. Consider implementing dialect-specific upsert.", UserWarning)
                    attribute_names = [record['attribute_name'] for record in serializable_data]
                    # Delete existing records first
                    if attribute_names:
                        delete_stmt = self.results_table.delete().where(self.results_table.c.attribute_name.in_(attribute_names)) # Use instance table
                        connection.execute(delete_stmt)
                    # Insert new records
                    if serializable_data:
                        connection.execute(self.results_table.insert(), serializable_data) # Use instance table
                    print(f"Successfully saved {len(serializable_data)} profiles using DELETE+INSERT.")

        except SQLAlchemyError as e:
            warnings.warn(f"Database error saving profiles: {e}", UserWarning)
        except Exception as e:
            warnings.warn(f"Unexpected error saving profiles: {e}", UserWarning)


    def get_all_profiles(self) -> pd.DataFrame:
        """Retrieves all profiles from the results table into a DataFrame."""
        try:
            query = sa.select(self.results_table) # Use instance table object
            with self.engine.connect() as connection:
                df = pd.read_sql(query, connection)
            print(f"Successfully retrieved {len(df)} profiles.")
            # Potentially deserialize JSON columns here if needed downstream
            return df
        except SQLAlchemyError as e:
            warnings.warn(f"Database error retrieving profiles: {e}", UserWarning)
            return pd.DataFrame() # Return empty DataFrame on error
        except Exception as e:
            warnings.warn(f"Unexpected error retrieving profiles: {e}", UserWarning)
            return pd.DataFrame()

    def update_cluster_id(self, attribute_name: str, cluster_id: int):
         """Updates the cluster_id for a specific attribute identifier."""
         try:
             stmt = self.results_table.update().where(
                 self.results_table.c.attribute_name == attribute_name
             ).values(cluster_id=cluster_id)
             with self.engine.begin() as connection:
                 result = connection.execute(stmt)
                 if result.rowcount == 0:
                      warnings.warn(f"Attribute '{attribute_name}' not found in results table for cluster ID update.")
                 # else:
                 #     print(f"Updated cluster ID for {attribute_name} to {cluster_id}")

         except SQLAlchemyError as e:
             warnings.warn(f"Database error updating cluster ID for '{attribute_name}': {e}", UserWarning)
         except Exception as e:
             warnings.warn(f"Unexpected error updating cluster ID for '{attribute_name}': {e}", UserWarning)


# Example Usage (for testing)
# if __name__ == '__main__':
#     # Requires a running DB and connection details
#     # from database_connector import DatabaseConnector
#     # db_details = {'db_type': 'postgresql', 'database': 'testdb', ...}
#     # engine = DatabaseConnector.create_db_engine(db_details)
#     # if engine:
#     #     results_manager = ResultsManager(engine)
#     #     # Create dummy profile data
#     #     dummy_profiles = [...]
#     #     results_manager.save_profiles(dummy_profiles)
#     #     all_data = results_manager.get_all_profiles()
#     #     print(all_data)
#     pass