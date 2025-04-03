# Project Plan: Python Data Profiler Tool

## 1. Goal

Develop a Python-based data profiling tool with a Streamlit UI that can:
*   Connect to databases (Snowflake, PostgreSQL) and CSV files using SQLAlchemy and Pandas.
*   Allow users to select attributes for profiling.
*   Calculate various profile metrics (stats, nulls, distinct values, etc.).
*   Detect specific patterns (SSN, Date of Birth) using detailed logic.
*   Store profiling results in a user-specified table (e.g., in Snowflake or PostgreSQL), overwriting previous results for the same attribute.
*   Perform similarity analysis and clustering (Hierarchical Clustering) on the profiled attributes stored in the results table, determining clusters based on similarity.
*   Present connection configuration, attribute selection, progress, and profiling/clustering results via the UI.

## 2. Architecture

```mermaid
graph TD
    A[User Interface (Streamlit)] --> B(Configuration Manager);
    A --> C(Database Connector);
    A --> D(Profiling Engine);
    A --> E(Results Manager);
    A --> F(Clustering Engine);

    C --> G{Source Databases (Snowflake, PostgreSQL, CSV)};
    D --> C;
    D --> H(Pattern Detector);
    D --> E;

    E --> I{Results Database/Table};
    F --> E;
    F --> A;

    subgraph Core Logic
        direction LR
        C
        D
        E
        F
        H
    end

    subgraph Data Stores
        direction TB
        G
        I
    end

    subgraph Presentation
        direction TB
        A
        B
    end

```

## 3. Technology Stack

*   **Backend/Core Logic:** Python 3.x
*   **UI:** Streamlit
*   **Database Abstraction:** SQLAlchemy
*   **Database Connectors/Drivers:** `snowflake-sqlalchemy`, `psycopg2-binary`
*   **Data Manipulation:** Pandas, NumPy
*   **Profiling Calculations:** Pandas, NumPy
*   **Pattern Detection:** Python `re` module, `python-dateutil`
*   **Clustering:** Scikit-learn (`sklearn.cluster`, `sklearn.preprocessing`)
*   **Results Storage:** Snowflake or PostgreSQL table (via SQLAlchemy).

## 4. Key Components

*   **User Interface (Streamlit):** Handles all user interactions: database connection setup (Snowflake, PostgreSQL, CSV path), results table info, attribute selection, displaying progress, showing profiling results (tables, charts), and clustering results.
*   **Configuration Manager:** Manages connection details and results table location securely.
*   **Database Connector (SQLAlchemy):** Uses SQLAlchemy to provide a unified interface for connecting to Snowflake and PostgreSQL. Uses Pandas for reading CSV files. Fetches data/metadata samples for profiling.
*   **Profiling Engine:** Takes a list of attributes and a connection object. For each attribute:
    *   Determines data type.
    *   Calculates relevant metrics (see section 5).
    *   Leverages the Pattern Detector (see section 6).
    *   Packages results.
*   **Pattern Detector:** Implements detailed logic for SSN and DOB detection (see section 6).
*   **Results Manager:** Handles writing profile results to the designated results table via SQLAlchemy. Manages overwriting logic based on attribute identifier and profile date. Provides data to the Clustering Engine and UI.
*   **Clustering Engine:** Reads current profiles from Results Manager. Prepares profile data (scaling). Applies Hierarchical Clustering (see section 7). Updates results table with cluster assignments.

## 5. Detailed Profiling Metrics

*   **Common Metrics:**
    *   `attribute_name`: Name of the column/attribute.
    *   `data_type_detected`: Profiler-inferred type (NUMERIC, STRING, DATETIME, BOOLEAN).
    *   `total_records`: Total records examined/sampled.
    *   `null_count`: Number of NULLs.
    *   `null_percentage`: Percentage of NULLs.
    *   `distinct_count`: Number of unique non-null values.
    *   `distinct_percentage`: Percentage of unique non-null values.
    *   `is_unique`: Boolean flag for uniqueness.
*   **Numeric Metrics:**
    *   `min`, `max`, `mean`, `median`, `std_dev`, `variance`.
    *   `quantiles`: Dictionary (e.g., 5th, 25th, 75th, 95th percentiles).
    *   `histogram`: Bin edges and counts.
*   **String/Text Metrics:**
    *   `min_length`, `max_length`, `avg_length`.
    *   `top_values`: List/dictionary of most frequent values and counts (for display).
    *   `is_ssn_candidate`: Boolean flag (from Pattern Detector).
    *   `is_dob_candidate`: Boolean flag (from Pattern Detector).
    *   `top_1_frequency_pct`: Frequency % of the most common value (for clustering).
    *   `top_5_frequency_pct`: Cumulative frequency % of the top 5 values (for clustering).
*   **Date/Time Metrics:**
    *   `min_date`, `max_date`, `time_range`.
    *   `common_formats`: Detected formats.
    *   `histogram_by_part`: Count by year, month, etc.
*   **Boolean Metrics:**
    *   `true_count`, `false_count`, `true_percentage`.

## 6. Detailed Pattern Detection Logic

*   **Date of Birth (DOB):**
    1.  **Name Check:** Look for "dob", "birth", "date_of_birth", "birthday" in attribute name.
    2.  **Format Check:** If name matches, sample values and attempt parsing with `dateutil.parser` or specific formats (YYYY-MM-DD, MM/DD/YYYY, etc.). Calculate % success.
    3.  **Logical Constraints:** If format success >90%, check parsed dates: not future, plausible age (0-120 years).
    4.  **Flag:** `True` if name matches, format success high, and logical constraints met.
*   **Social Security Number (SSN):**
    1.  **Name Check (Optional):** Look for "ssn", "social", "security".
    2.  **Format Check:** Sample values. Check regex `^\d{3}-\d{2}-\d{4}$` or `^\d{9}$`. Calculate % success.
    3.  **Logical Constraints:** If format success >90%, check format-matching values: Area Number (first 3) not 000, 666, 900-999; Group Number (middle 2) not 00; Serial Number (last 4) not 0000. Calculate % passing constraints.
    4.  **Flag:** `True` if format success high and logical constraint pass rate high.

## 7. Detailed Clustering Algorithm

*   **Algorithm:** Agglomerative Hierarchical Clustering (`sklearn.cluster.AgglomerativeClustering`).
*   **Features:** Scaled numerical profile metrics (including `null_percentage`, `distinct_percentage`, numeric stats, length stats, `is_ssn_candidate` (0/1), `is_dob_candidate` (0/1), `top_1_frequency_pct`, `top_5_frequency_pct`).
*   **Scaling:** Use `StandardScaler` on features before clustering.
*   **Distance Metric:** `euclidean` (default, good starting point).
*   **Linkage Criterion:** `ward` (often effective, minimizes within-cluster variance).
*   **Cluster Determination:** Use `distance_threshold` instead of `n_clusters`. The algorithm stops merging when the linkage distance exceeds this threshold. The threshold value might require tuning or heuristics based on dendrogram analysis.
*   **Output:** Cluster ID assigned to each attribute in the results table.

## 8. Data Flow

1.  **Configure:** User provides DB/CSV connection details and results table info via UI.
2.  **Connect:** UI triggers Database Connector (SQLAlchemy/Pandas).
3.  **Select:** User selects schema/table/attributes via UI.
4.  **Profile:** UI passes selection to Profiling Engine.
5.  **Fetch & Analyze:** Profiling Engine uses Connector to get data/metadata, calculates metrics, uses Pattern Detector.
6.  **Store:** Profiling Engine sends results to Results Manager, which writes/overwrites data in results table via SQLAlchemy.
7.  **Cluster:** UI triggers Clustering Engine.
8.  **Analyze Results:** Clustering Engine reads profiles from Results Manager, scales features, performs clustering, updates results table with cluster IDs via Results Manager.
9.  **Display:** UI reads from Results Manager to display profiles and cluster info.

## 9. Key Considerations

*   **Performance:** Sampling for large tables, potential for async processing (may require more than basic Streamlit).
*   **Schema Evolution:** Handling source schema changes (future consideration).
*   **Data Types:** Handling diverse types across DBs. SQLAlchemy helps.
*   **Clustering Scalability:** Performance with thousands of attributes.
*   **Configuration Security:** Secure storage/handling of credentials.
*   **Error Handling:** Robust handling for connections, SQL, data issues.
*   **Dependency Management:** Clear `requirements.txt`.