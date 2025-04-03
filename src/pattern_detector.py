import re
import pandas as pd
from dateutil.parser import parse
from datetime import datetime

# --- SSN Detection Logic ---

# Pre-compiled regex for efficiency
SSN_REGEX_HYPHEN = re.compile(r"^\d{3}-\d{2}-\d{4}$")
SSN_REGEX_NO_HYPHEN = re.compile(r"^\d{9}$")

# Invalid Area Numbers (first 3 digits)
INVALID_SSN_AREA = {"000", "666"} | set(str(i) for i in range(900, 1000))

def _is_valid_ssn_logical(ssn_str: str) -> bool:
    """Checks logical constraints for a structurally valid SSN string."""
    if len(ssn_str) == 9:
        area = ssn_str[0:3]
        group = ssn_str[3:5]
        serial = ssn_str[5:9]
    elif len(ssn_str) == 11: # Format with hyphens
        area = ssn_str[0:3]
        group = ssn_str[4:6]
        serial = ssn_str[7:11]
    else:
        return False # Should not happen if pre-validated

    if area in INVALID_SSN_AREA:
        return False
    if group == "00":
        return False
    if serial == "0000":
        return False
    return True

def check_ssn_candidate(series: pd.Series, sample_size: int = 100, format_threshold: float = 0.9, logical_threshold: float = 0.95) -> bool:
    """
    Checks if a pandas Series likely contains SSN values based on format and logic.

    Args:
        series: The pandas Series to check (expected to contain strings).
        sample_size: The number of non-null values to sample for checking.
        format_threshold: The minimum proportion of sampled values that must match SSN format.
        logical_threshold: The minimum proportion of format-matching values that must pass logical checks.

    Returns:
        True if the series is a likely SSN candidate, False otherwise.
    """
    non_null_series = series.dropna().astype(str)
    if non_null_series.empty:
        return False

    # Sample data for performance
    sample = non_null_series.sample(n=min(sample_size, len(non_null_series)), random_state=42) # Use fixed random state for reproducibility

    format_matches = 0
    logical_passes = 0
    format_matching_values = []

    for value in sample:
        is_format_match = False
        cleaned_value = value.strip()
        if SSN_REGEX_HYPHEN.match(cleaned_value) or SSN_REGEX_NO_HYPHEN.match(cleaned_value):
            is_format_match = True
            format_matches += 1
            format_matching_values.append(cleaned_value)

    if format_matches == 0:
        return False

    format_match_ratio = format_matches / len(sample)
    if format_match_ratio < format_threshold:
        return False

    # Check logical constraints only on values that matched the format
    for ssn_str in format_matching_values:
        if _is_valid_ssn_logical(ssn_str):
            logical_passes += 1

    if format_matches > 0: # Avoid division by zero
        logical_pass_ratio = logical_passes / format_matches
        if logical_pass_ratio >= logical_threshold:
            return True
    
    return False


# --- Date of Birth (DOB) Detection Logic ---

DOB_NAME_KEYWORDS = {"dob", "birth", "date_of_birth", "birthday"}

def _is_plausible_dob(dt: datetime) -> bool:
    """Checks if a datetime object is a plausible DOB."""
    now = datetime.now()
    if dt > now: # Cannot be born in the future
        return False
    age = (now - dt).days / 365.25
    if age < 0 or age > 120: # Plausible age range
        return False
    return True

def check_dob_candidate(series: pd.Series, attribute_name: str, sample_size: int = 100, format_threshold: float = 0.9, logical_threshold: float = 0.95) -> bool:
    """
    Checks if a pandas Series likely contains Date of Birth values.

    Args:
        series: The pandas Series to check.
        attribute_name: The name of the attribute/column.
        sample_size: The number of non-null values to sample for checking.
        format_threshold: The minimum proportion of sampled values that must parse as dates.
        logical_threshold: The minimum proportion of parsed dates that must be plausible DOBs.

    Returns:
        True if the series is a likely DOB candidate, False otherwise.
    """
    # 1. Name Check
    if not any(keyword in attribute_name.lower() for keyword in DOB_NAME_KEYWORDS):
        return False # Skip if name doesn't suggest DOB

    non_null_series = series.dropna()
    if non_null_series.empty:
        return False

    # Ensure series is treated as string for parsing attempt
    str_series = non_null_series.astype(str)

    # Sample data for performance
    sample = str_series.sample(n=min(sample_size, len(str_series)), random_state=42)

    parsed_dates = []
    parse_success = 0
    parse_attempts = 0

    # 2. Format Check (using dateutil.parser for flexibility)
    for value in sample:
        parse_attempts += 1
        try:
            # Use fuzzy=False to avoid overly lenient parsing if needed, but start flexible
            parsed_date = parse(value, fuzzy=False) 
            parsed_dates.append(parsed_date)
            parse_success += 1
        except (ValueError, TypeError, OverflowError):
            # Handle potential errors during parsing
            pass 

    if parse_attempts == 0: # Should not happen if non_null_series wasn't empty
        return False

    parse_success_ratio = parse_success / parse_attempts
    if parse_success_ratio < format_threshold:
        return False

    # 3. Logical Constraints
    plausible_count = 0
    for dt in parsed_dates:
        if _is_plausible_dob(dt):
            plausible_count += 1

    if parse_success > 0: # Avoid division by zero
        plausible_ratio = plausible_count / parse_success
        if plausible_ratio >= logical_threshold:
            # 4. Flag Assignment
            return True

    return False
