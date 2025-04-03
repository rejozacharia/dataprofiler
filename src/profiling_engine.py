import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
from src.pattern_detector import check_ssn_candidate, check_dob_candidate
import warnings

# Define constants for data types
DTYPE_NUMERIC = "NUMERIC"
DTYPE_STRING = "STRING"
DTYPE_DATETIME = "DATETIME"
DTYPE_BOOLEAN = "BOOLEAN"
DTYPE_UNSUPPORTED = "UNSUPPORTED"
DTYPE_MIXED = "MIXED" # Added for columns with multiple inferred types after coercion

# Quantiles to calculate for numeric types
QUANTILES = [0.05, 0.25, 0.5, 0.75, 0.95]

def infer_dtype(series: pd.Series) -> str:
    """Infers the logical data type of a pandas Series, handling mixed types."""
    # Drop NaNs for type inference, but keep original series for later checks
    series_non_null = series.dropna()
    if series_non_null.empty:
        # If all nulls, classify as unsupported
        return DTYPE_UNSUPPORTED

    original_dtype = series.dtype

    # --- Direct Type Checks ---
    if pd.api.types.is_bool_dtype(original_dtype):
        return DTYPE_BOOLEAN
    if pd.api.types.is_datetime64_any_dtype(original_dtype) or pd.api.types.is_timedelta64_dtype(original_dtype):
        return DTYPE_DATETIME
    if pd.api.types.is_numeric_dtype(original_dtype) and not pd.api.types.is_bool_dtype(original_dtype):
         # Check if it looks like boolean despite numeric type (e.g., 0/1 int column)
         unique_vals = series_non_null.unique()
         if len(unique_vals) <= 2 and all(v in [0, 1] for v in unique_vals):
             return DTYPE_BOOLEAN # Treat 0/1 integer columns as boolean
         return DTYPE_NUMERIC
    if pd.api.types.is_string_dtype(original_dtype) or pd.api.types.is_categorical_dtype(original_dtype):
        # Try converting string types to numeric or datetime
        try:
            # Attempt numeric conversion first
            pd.to_numeric(series_non_null, errors='raise')
            # If entirely convertible to numeric, classify as numeric
            return DTYPE_NUMERIC
        except (ValueError, TypeError):
            # Not entirely numeric, try datetime
            try:
                # Use a sample for performance check
                sample_size = min(100, len(series_non_null))
                sample = series_non_null.sample(sample_size, random_state=42) if sample_size > 0 else pd.Series(dtype=object)
                if sample.empty: return DTYPE_STRING # Only nulls or empty strings
                pd.to_datetime(sample, errors='raise')
                # If sample converts to datetime, tentatively classify as datetime
                # More robust: check conversion percentage on full data if needed
                # Let's assume if a sample parses, it's likely datetime for now
                return DTYPE_DATETIME
            except (ValueError, TypeError, OverflowError):
                 # If not numeric or datetime, it's likely string
                 return DTYPE_STRING

    # --- Handle Object Dtype (most complex) ---
    if original_dtype == object:
        # Attempt conversions and see what sticks
        numeric_coerced = pd.to_numeric(series_non_null, errors='coerce')
        datetime_coerced = pd.to_datetime(series_non_null, errors='coerce')

        not_na_numeric = numeric_coerced.notna()
        not_na_datetime = datetime_coerced.notna()

        all_numeric = not_na_numeric.all()
        all_datetime = not_na_datetime.all()

        if all_numeric:
            # Check if it looks boolean (0/1)
            unique_vals = numeric_coerced.dropna().unique()
            if len(unique_vals) <= 2 and all(v in [0, 1] for v in unique_vals):
                return DTYPE_BOOLEAN
            return DTYPE_NUMERIC
        if all_datetime:
            return DTYPE_DATETIME

        # Check for boolean-like strings ('True', 'False', 'Yes', 'No', etc.)
        try:
            # Use a robust check for boolean-like values
            bool_map = {'true': True, 'false': False, 'yes': True, 'no': False, '1': True, '0': False, 't': True, 'f': False, 'y': True, 'n': False}
            # Check if ALL non-null values can be mapped
            mapped_bools = series_non_null.astype(str).str.lower().map(bool_map)
            if mapped_bools.notna().all(): # If all values map successfully to boolean
                 return DTYPE_BOOLEAN
        except Exception:
            pass # Ignore errors during bool check

        # Default to STRING if object and not clearly numeric/datetime/boolean
        return DTYPE_STRING

    return DTYPE_UNSUPPORTED


def profile_attribute(series: pd.Series, attribute_name: str) -> Optional[Dict[str, Any]]:
    """
    Calculates profile metrics for a single pandas Series (attribute).

    Args:
        series: The pandas Series representing the attribute data.
        attribute_name: The name of the attribute.

    Returns:
        A dictionary containing the profile metrics, or None if type is unsupported.
    """
    profile = {"attribute_name": attribute_name}
    total_records = len(series)
    profile["total_records"] = total_records

    # Handle completely empty series
    if total_records == 0:
        profile["data_type_detected"] = DTYPE_UNSUPPORTED
        profile["null_count"] = 0
        profile["null_percentage"] = 0.0
        return profile

    # --- Common Metrics ---
    null_count = series.isnull().sum()
    profile["null_count"] = int(null_count)
    profile["null_percentage"] = (null_count / total_records) * 100 if total_records > 0 else 0.0

    non_null_series = series.dropna()
    non_null_count = len(non_null_series) # Count after dropping nulls

    if non_null_count == 0: # All values were null
        profile["data_type_detected"] = DTYPE_UNSUPPORTED
        profile["distinct_count"] = 0
        profile["distinct_percentage"] = 0.0
        profile["is_unique"] = True
        return profile

    distinct_count = non_null_series.nunique()
    profile["distinct_count"] = int(distinct_count)
    profile["distinct_percentage"] = (distinct_count / non_null_count) * 100 if non_null_count > 0 else 0.0 # Avoid division by zero
    profile["is_unique"] = (distinct_count == non_null_count)

    # --- Type-Specific Metrics ---
    dtype = infer_dtype(series)
    profile["data_type_detected"] = dtype

    # --- Numeric Profiling ---
    if dtype == DTYPE_NUMERIC:
        # Coerce to numeric, errors become NaN, then drop these NaNs
        numeric_series = pd.to_numeric(non_null_series, errors='coerce').dropna()
        if not numeric_series.empty:
            profile["min"] = float(numeric_series.min())
            profile["max"] = float(numeric_series.max())
            profile["mean"] = float(numeric_series.mean())
            profile["median"] = float(numeric_series.median())
            profile["std_dev"] = float(numeric_series.std()) if len(numeric_series) > 1 else 0.0 # Std needs > 1 point
            profile["variance"] = float(numeric_series.var()) if len(numeric_series) > 1 else 0.0 # Var needs > 1 point
            try:
                quantiles_dict = numeric_series.quantile(QUANTILES).to_dict()
                profile["quantiles"] = {f"{int(q*100)}th": float(v) for q, v in quantiles_dict.items()}
            except Exception as e:
                 warnings.warn(f"Could not compute quantiles for {attribute_name}: {e}")
                 profile["quantiles"] = {}
            try:
                counts, bin_edges = np.histogram(numeric_series, bins='auto')
                profile["histogram"] = {"counts": counts.tolist(), "bin_edges": bin_edges.tolist()}
            except Exception as e:
                warnings.warn(f"Could not compute histogram for {attribute_name}: {e}")
                profile["histogram"] = None

    # --- String Profiling ---
    elif dtype == DTYPE_STRING:
        string_series = non_null_series.astype(str)
        lengths = string_series.str.len()
        profile["min_length"] = int(lengths.min())
        profile["max_length"] = int(lengths.max())
        profile["avg_length"] = float(lengths.mean())
        try:
            value_counts = string_series.value_counts()
            top_n = min(10, len(value_counts))
            profile["top_values"] = {str(k): int(v) for k, v in value_counts.head(top_n).items()}
            if not value_counts.empty:
                 profile["top_1_frequency_pct"] = (value_counts.iloc[0] / non_null_count) * 100
                 profile["top_5_frequency_pct"] = (value_counts.head(5).sum() / non_null_count) * 100
            else:
                 profile["top_1_frequency_pct"] = 0.0
                 profile["top_5_frequency_pct"] = 0.0
        except Exception as e:
            warnings.warn(f"Could not compute frequency for {attribute_name}: {e}")
            profile["top_values"] = {}
            profile["top_1_frequency_pct"] = 0.0
            profile["top_5_frequency_pct"] = 0.0
        # Pattern detection on the original non-null series (before explicit string conversion)
        profile["is_ssn_candidate"] = check_ssn_candidate(non_null_series)
        profile["is_dob_candidate"] = check_dob_candidate(non_null_series, attribute_name)

    # --- Datetime Profiling ---
    elif dtype == DTYPE_DATETIME:
         # Coerce to datetime, errors become NaT, then drop NaTs
         datetime_series = pd.to_datetime(non_null_series, errors='coerce').dropna()
         if not datetime_series.empty:
             profile["min_date"] = datetime_series.min().isoformat()
             profile["max_date"] = datetime_series.max().isoformat()
             try:
                 # Ensure both min and max are valid timestamps before calculating range
                 if pd.notna(datetime_series.min()) and pd.notna(datetime_series.max()):
                     time_range = datetime_series.max() - datetime_series.min()
                     profile["time_range_days"] = float(time_range.total_seconds() / (24 * 3600))
                 else:
                     profile["time_range_days"] = None
             except TypeError as e:
                 warnings.warn(f"Could not compute time range for {attribute_name} (mixed timezones?): {e}")
                 profile["time_range_days"] = None
             try:
                 year_counts = datetime_series.dt.year.value_counts().sort_index()
                 profile["histogram_by_year"] = {str(k): int(v) for k, v in year_counts.items()}
             except Exception as e:
                 warnings.warn(f"Could not compute year histogram for {attribute_name}: {e}")
                 profile["histogram_by_year"] = None

    # --- Boolean Profiling ---
    elif dtype == DTYPE_BOOLEAN:
        # Robust conversion to boolean
        bool_series = None
        temp_series = non_null_series # Work on non-nulls
        if pd.api.types.is_string_dtype(temp_series.dtype) or temp_series.dtype == object:
             bool_map = {'true': True, 'false': False, 'yes': True, 'no': False, '1': True, '0': False, 't': True, 'f': False, 'y': True, 'n': False}
             # Map known bool strings, leave others as NaN
             mapped_series = temp_series.astype(str).str.lower().map(bool_map)
             # Only consider values that successfully mapped
             bool_series = mapped_series.dropna().astype(bool)
        elif pd.api.types.is_numeric_dtype(temp_series.dtype):
             # Assume 0 is False, 1 is True for numeric -> bool if inferred as boolean
             unique_vals = temp_series.unique()
             if len(unique_vals) <= 2 and all(v in [0, 1] for v in unique_vals):
                 bool_series = temp_series.astype(bool)
             else: # If numeric but not 0/1, conversion to bool is ambiguous
                 bool_series = pd.Series(dtype=bool) # Empty boolean series
        else: # Already boolean
             bool_series = temp_series.astype(bool)

        # Recalculate count based on successfully converted booleans
        final_bool_count = len(bool_series)
        if final_bool_count > 0:
            true_count = bool_series.sum()
            false_count = final_bool_count - true_count
            profile["true_count"] = int(true_count)
            profile["false_count"] = int(false_count)
            profile["true_percentage"] = (true_count / final_bool_count) * 100
        else: # Handle case where no values could be converted to boolean
            profile["true_count"] = 0
            profile["false_count"] = 0
            profile["true_percentage"] = 0.0

    # --- Unsupported Type ---
    elif dtype == DTYPE_UNSUPPORTED:
        warnings.warn(f"Unsupported data type for attribute '{attribute_name}'. Skipping detailed profiling.")
        # Return basic profile info only
        basic_profile = {k: profile.get(k) for k in ["attribute_name", "total_records", "null_count", "null_percentage", "data_type_detected", "distinct_count", "distinct_percentage", "is_unique"]}
        return basic_profile

    return profile


def profile_dataframe(df: pd.DataFrame, columns_to_profile: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """
    Profiles specified columns in a pandas DataFrame.

    Args:
        df: The input DataFrame.
        columns_to_profile: A list of column names to profile. If None, profiles all columns.

    Returns:
        A list of dictionaries, where each dictionary contains the profile metrics for one attribute.
    """
    if columns_to_profile is None:
        columns_to_profile = df.columns.tolist()

    all_profiles = []
    for col_name in columns_to_profile:
        if col_name in df.columns:
            print(f"Profiling column: {col_name}...")
            try:
                # Make a copy to avoid modifying original DataFrame during type inference/coercion
                profile = profile_attribute(df[col_name].copy(), col_name)
                if profile:
                    all_profiles.append(profile)
            except Exception as e:
                 warnings.warn(f"ERROR: Failed to profile column '{col_name}': {e}", UserWarning)
                 # Add a basic error profile
                 error_profile = {
                     "attribute_name": col_name,
                     "total_records": len(df),
                     "error": str(e)
                 }
                 # Try to add basic stats even if full profile failed
                 try:
                     error_profile["null_count"] = int(df[col_name].isnull().sum())
                     error_profile["null_percentage"] = (error_profile["null_count"] / error_profile["total_records"]) * 100 if error_profile["total_records"] > 0 else 0.0
                 except Exception:
                     pass # Ignore errors during error reporting
                 all_profiles.append(error_profile)
        else:
            warnings.warn(f"Column '{col_name}' not found in DataFrame. Skipping.")

    print("DataFrame profiling complete.")
    return all_profiles

# Example Usage (for testing purposes)
# if __name__ == '__main__':
#     # Add test data and calls here if needed
#     pass
