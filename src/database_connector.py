import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import SQLAlchemyError
import warnings
from typing import Optional, Dict, Any, List

class DatabaseConnector:
    """Handles database connections and basic data retrieval."""

    @staticmethod
    def create_db_engine(conn_details: Dict[str, Any]) -> Optional[Engine]:
        """
        Creates a SQLAlchemy engine based on connection details.

        Args:
            conn_details: A dictionary containing connection parameters.
                          Expected keys vary by db_type:
                          - 'db_type': 'snowflake', 'postgresql'
                          - 'username', 'password'
                          - 'host', 'port', 'database' (for postgresql)
                          - 'account', 'warehouse', 'database', 'schema' (for snowflake)
                          - Optional: 'role' (for snowflake)

        Returns:
            A SQLAlchemy Engine object or None if connection fails.
        """
        db_type = conn_details.get("db_type")
        engine = None

        try:
            if db_type == "postgresql":
                url = URL.create(
                    drivername="postgresql+psycopg2",
                    username=conn_details.get("username"),
                    password=conn_details.get("password"),
                    host=conn_details.get("host"),
                    port=conn_details.get("port"),
                    database=conn_details.get("database"),
                )
                engine = sa.create_engine(url)
                # Test connection
                with engine.connect() as connection:
                    print(f"Successfully connected to PostgreSQL: {conn_details.get('database')}")

            elif db_type == "snowflake":
                url = URL.create(
                    drivername="snowflake",
                    username=conn_details.get("username"),
                    password=conn_details.get("password"),
                    account=conn_details.get("account"),
                    warehouse=conn_details.get("warehouse"),
                    database=conn_details.get("database"),
                    schema=conn_details.get("schema"),
                    role=conn_details.get("role"), # Optional
                )
                engine = sa.create_engine(url)
                # Test connection
                with engine.connect() as connection:
                    print(f"Successfully connected to Snowflake: {conn_details.get('database')}.{conn_details.get('schema')}")
            
            else:
                warnings.warn(f"Unsupported database type: {db_type}")
                return None

            return engine

        except ImportError as e:
             warnings.warn(f"Missing database driver for {db_type}: {e}. Please install required packages.")
             return None
        except SQLAlchemyError as e:
            warnings.warn(f"Database connection error for {db_type}: {e}")
            return None
        except Exception as e:
            warnings.warn(f"An unexpected error occurred during connection setup for {db_type}: {e}")
            return None

    @staticmethod
    def get_schemas(engine: Engine) -> List[str]:
        """Gets a list of accessible schemas (excluding system schemas)."""
        try:
            inspector = sa.inspect(engine)
            schemas = inspector.get_schema_names()
            # Filter out common system schemas
            system_schemas = {'information_schema', 'pg_catalog', 'pg_toast'} # Add more if needed
            user_schemas = [s for s in schemas if s.lower() not in system_schemas]
            return user_schemas
        except SQLAlchemyError as e:
            warnings.warn(f"Error retrieving schemas: {e}")
            return []

    @staticmethod
    def get_tables(engine: Engine, schema: Optional[str] = None) -> List[str]:
        """Gets a list of tables within a specific schema."""
        try:
            inspector = sa.inspect(engine)
            return inspector.get_table_names(schema=schema)
        except SQLAlchemyError as e:
            warnings.warn(f"Error retrieving tables for schema '{schema}': {e}")
            return []

    @staticmethod
    def get_columns(engine: Engine, table_name: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """Gets column names and types for a specific table."""
        try:
            inspector = sa.inspect(engine)
            return inspector.get_columns(table_name, schema=schema)
        except SQLAlchemyError as e:
            warnings.warn(f"Error retrieving columns for table '{schema}.{table_name}': {e}")
            return []

    @staticmethod
    def get_table_sample(engine: Engine, table_name: str, schema: Optional[str] = None, sample_size: int = 1000, is_random: bool = True) -> Optional[pd.DataFrame]:
        """
        Retrieves a sample of data from a table into a Pandas DataFrame.

        Args:
            engine: SQLAlchemy engine.
            table_name: Name of the table.
            schema: Schema of the table (optional).
            sample_size: Number of rows to sample.
            is_random: Whether to perform random sampling (can be slower).

        Returns:
            A Pandas DataFrame containing the sample, or None on error.
        """
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        try:
            if is_random:
                # Random sampling can be database-specific and potentially slow
                # Using TABLESAMPLE BERNOULLI for broader compatibility (if supported)
                # Fallback to LIMIT if TABLESAMPLE fails or isn't needed
                try:
                    # Note: TABLESAMPLE syntax varies (e.g., SYSTEM vs BERNOULLI)
                    # This is a guess; might need adjustment per DB
                    query = sa.text(f"SELECT * FROM {full_table_name} TABLESAMPLE SYSTEM (1) LIMIT :n") 
                    # Adjust percentage based on sample_size and estimated table size if possible
                    # For simplicity, using LIMIT as a fallback / primary method for now
                    query = sa.text(f"SELECT * FROM {full_table_name} LIMIT :n")
                    df = pd.read_sql(query, engine, params={'n': sample_size})

                except SQLAlchemyError: # Fallback if TABLESAMPLE fails
                     warnings.warn("TABLESAMPLE failed or not supported, using simple LIMIT.")
                     query = sa.text(f"SELECT * FROM {full_table_name} LIMIT :n")
                     df = pd.read_sql(query, engine, params={'n': sample_size})
            else:
                 query = sa.text(f"SELECT * FROM {full_table_name} LIMIT :n")
                 df = pd.read_sql(query, engine, params={'n': sample_size})

            print(f"Successfully sampled {len(df)} rows from {full_table_name}")
            return df
        except SQLAlchemyError as e:
            warnings.warn(f"Error sampling data from table '{full_table_name}': {e}")
            return None
        except Exception as e:
            warnings.warn(f"An unexpected error occurred during sampling from '{full_table_name}': {e}")
            return None


    @staticmethod
    def read_csv(file_path: str, **kwargs) -> Optional[pd.DataFrame]:
        """Reads a CSV file into a Pandas DataFrame."""
        try:
            df = pd.read_csv(file_path, **kwargs)
            print(f"Successfully read CSV: {file_path}")
            return df
        except FileNotFoundError:
            warnings.warn(f"CSV file not found: {file_path}")
            return None
        except Exception as e:
            warnings.warn(f"Error reading CSV file '{file_path}': {e}")
            return None

# Example Usage (for testing)
if __name__ == '__main__':
    # Add example connection details and test calls here
    # e.g., pg_details = {'db_type': 'postgresql', ...}
    # engine = DatabaseConnector.create_db_engine(pg_details)
    # if engine:
    #     schemas = DatabaseConnector.get_schemas(engine)
    #     print("Schemas:", schemas)
    pass