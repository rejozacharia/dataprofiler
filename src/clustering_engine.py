import pandas as pd
import numpy as np
from sklearn.cluster import AgglomerativeClustering
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from typing import List, Dict, Any, Optional, Tuple
import warnings

# Import ResultsManager to interact with profile data
from src.results_manager import ResultsManager


class ClusteringEngine:
    """Performs clustering on profiled attribute data."""

    def __init__(self, results_manager: ResultsManager):
        """
        Initializes the ClusteringEngine.

        Args:
            results_manager: An instance of ResultsManager to get profile data
                             and update cluster IDs.
        """
        if not isinstance(results_manager, ResultsManager):
            raise TypeError("results_manager must be an instance of ResultsManager.")
        self.results_manager = results_manager

        # Define the features to be used for clustering based on PROJECT_PLAN.md
        # Focus on numerical metrics and boolean flags (converted to 0/1)
        self.clustering_features = [
            # Common
            'null_percentage', 'distinct_percentage',
            # Numeric (use representative stats)
            'mean', 'median', 'std_dev',
            # String/Text
            'avg_length', 'is_ssn_candidate', 'is_dob_candidate',
            'top_1_frequency_pct', 'top_5_frequency_pct',
            # Date/Time (use range?)
            'time_range_days',
            # Boolean
            'true_percentage'
        ]
        # Note: Selection of features might need refinement based on results

    def _prepare_data_for_clustering(self, df: pd.DataFrame) -> Tuple[Optional[pd.DataFrame], Optional[List[str]]]:
        """
        Selects features, handles missing values, and scales data.

        Args:
            df: DataFrame containing profile data from ResultsManager.

        Returns:
            A tuple containing:
            - Scaled data as a NumPy array or None if preparation fails.
            - List of attribute names corresponding to the rows in the scaled data or None.
        """
        if 'attribute_name' not in df.columns:
            warnings.warn("Missing 'attribute_name' column in profile data. Cannot perform clustering.", UserWarning)
            return None, None

        # Filter out profiles with errors if an 'error' column exists and is not null
        if 'error' in df.columns:
            df_valid = df[df['error'].isnull()].copy()
            if len(df_valid) < len(df):
                 warnings.warn(f"Excluded {len(df) - len(df_valid)} profiles with errors from clustering.", UserWarning)
        else:
            df_valid = df.copy()

        if df_valid.empty:
            warnings.warn("No valid profiles available for clustering.", UserWarning)
            return None, None

        # Select only the features relevant for clustering
        features_present = [f for f in self.clustering_features if f in df_valid.columns]
        if not features_present:
            warnings.warn("None of the specified clustering features found in the profile data.", UserWarning)
            return None, None

        data_to_cluster = df_valid[['attribute_name'] + features_present].copy()

        # Convert boolean flags to numeric (0/1)
        for col in ['is_ssn_candidate', 'is_dob_candidate']:
            if col in data_to_cluster.columns:
                # Fill NA before converting to int, then bool, then int (0/1)
                data_to_cluster[col] = data_to_cluster[col].fillna(False).astype(bool).astype(int)

        # Separate attribute names and feature matrix
        attribute_names = data_to_cluster['attribute_name'].tolist()
        feature_matrix = data_to_cluster[features_present]

        # Handle missing values (imputation)
        # Using median imputation as a robust strategy for skewed distributions
        try:
            imputer = SimpleImputer(strategy='median')
            imputed_matrix = imputer.fit_transform(feature_matrix)
            n_imputed = np.isnan(feature_matrix.values).sum()
            if n_imputed > 0:
                 warnings.warn(f"Imputed {n_imputed} missing values using median strategy.", UserWarning)
        except Exception as e:
            warnings.warn(f"Error during imputation: {e}. Cannot proceed with clustering.", UserWarning)
            return None, None


        # Scale the features
        try:
            scaler = StandardScaler()
            scaled_matrix = scaler.fit_transform(imputed_matrix)
        except Exception as e:
            warnings.warn(f"Error during scaling: {e}. Cannot proceed with clustering.", UserWarning)
            return None, None

        return scaled_matrix, attribute_names


    def perform_clustering(self, distance_threshold: float = 5.0) -> Optional[pd.DataFrame]:
        """
        Performs hierarchical clustering on the profile data.

        Args:
            distance_threshold: The linkage distance threshold above which clusters
                                will not be merged. Controls the number of clusters.

        Returns:
            DataFrame with attribute names and assigned cluster IDs, or None if clustering fails.
        """
        print("Starting clustering process...")
        profile_df = self.results_manager.get_all_profiles()

        if profile_df.empty:
            warnings.warn("No profile data found to perform clustering.", UserWarning)
            return None

        scaled_data, attribute_names = self._prepare_data_for_clustering(profile_df)

        if scaled_data is None or attribute_names is None:
            warnings.warn("Data preparation failed. Aborting clustering.", UserWarning)
            return None
            
        if len(scaled_data) < 2:
             warnings.warn("Need at least 2 data points to perform clustering. Aborting.", UserWarning)
             # Assign cluster 0 to the single point?
             if len(scaled_data) == 1:
                 self.results_manager.update_cluster_id(attribute_names[0], 0)
                 print("Assigned cluster 0 to the single attribute.")
                 return pd.DataFrame({'attribute_name': attribute_names, 'cluster_id': [0]})
             return None


        print(f"Performing Agglomerative Clustering on {len(scaled_data)} attributes using {scaled_data.shape[1]} features...")
        try:
            # Perform clustering
            # Using n_clusters=None and distance_threshold
            model = AgglomerativeClustering(
                n_clusters=None,
                distance_threshold=distance_threshold,
                linkage='ward', # Minimizes variance within clusters
                affinity='euclidean' # Standard distance metric
            )
            cluster_labels = model.fit_predict(scaled_data)
            n_clusters_found = len(set(cluster_labels))
            print(f"Clustering complete. Found {n_clusters_found} clusters with distance threshold {distance_threshold}.")

            # Update results table with cluster IDs
            print("Updating results table with cluster IDs...")
            updates_failed = 0
            for attr_name, cluster_id in zip(attribute_names, cluster_labels):
                try:
                    self.results_manager.update_cluster_id(attr_name, int(cluster_id))
                except Exception as e:
                     warnings.warn(f"Failed to update cluster ID for {attr_name}: {e}", UserWarning)
                     updates_failed += 1
            
            if updates_failed > 0:
                 warnings.warn(f"Failed to update cluster IDs for {updates_failed} attributes.", UserWarning)
            else:
                 print("Successfully updated all cluster IDs.")


            # Return results
            results_df = pd.DataFrame({
                'attribute_name': attribute_names,
                'cluster_id': cluster_labels
            })
            return results_df

        except Exception as e:
            warnings.warn(f"An error occurred during clustering: {e}", UserWarning)
            return None

# Example Usage (for testing)
# if __name__ == '__main__':
#     # Requires a running DB with results and connection details
#     # from database_connector import DatabaseConnector
#     # db_details = {'db_type': 'postgresql', 'database': 'testdb', ...}
#     # engine = DatabaseConnector.create_db_engine(db_details)
#     # if engine:
#     #     results_manager = ResultsManager(engine)
#     #     # Ensure some data exists in the results table first
#     #     clustering_engine = ClusteringEngine(results_manager)
#     #     cluster_results = clustering_engine.perform_clustering(distance_threshold=10.0) # Adjust threshold as needed
#     #     if cluster_results is not None:
#     #         print("Clustering Results:")
#     #         print(cluster_results)
#     #         # Verify updates in the DB
#     #         print("\nVerifying DB update:")
#     #         print(results_manager.get_all_profiles()[['attribute_name', 'cluster_id']])
#     pass