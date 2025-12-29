/* =========================================================
   Recs+ catalog field analysis
   One-query, retailer-scoped
   Output is designed for CSV export and Gemini formatting
   Note: custom columns that are always null are not included.
   ========================================================= */

WITH params AS (
  SELECT 1457::NUMBER AS retailer_id   -- <<< CHANGE THIS ONLY
),

/* Anchor retailer name to exactly one row */
retailer AS (
  SELECT
    ca.retailer_id,
    ca.retailer_name
  FROM config_account ca
  JOIN params p
    ON ca.retailer_id = p.retailer_id
  WHERE ca.archived = false
  QUALIFY ROW_NUMBER() OVER (ORDER BY ca.retailer_name) = 1
),

/* Current catalog rows using your existing cutoff and archive logic */
latest_catalog AS (
  SELECT c.*
  FROM product_catalog c
  JOIN config_dataset_data_expiration ex
    ON ex.dataset_id = c.dataset_id
  JOIN config_dataset_info i
    ON i.dataset_id = c.dataset_id
  JOIN params p
    ON c.retailer_id = p.retailer_id
  WHERE c.update_time >= ex.cutoff_time
    AND i.archive_time IS NULL
),

/* Standard fields as an object, excluding a small set of internal keys */
standard_src AS (
  SELECT
    c.retailer_id,
    c.dataset_id,
    c.id AS product_id,
    OBJECT_DELETE(
      OBJECT_DELETE(
        OBJECT_DELETE(
          OBJECT_DELETE(
            OBJECT_CONSTRUCT_KEEP_NULL(*),
            'CUSTOM'
          ),
          'RETAILER_ID'
        ),
        'DATASET_ID'
      ),
      'UPDATE_TIME'
    ) AS src
  FROM latest_catalog c
),

/* Custom fields as an object */
custom_src AS (
  SELECT
    c.retailer_id,
    c.dataset_id,
    c.id AS product_id,
    c.custom AS src
  FROM latest_catalog c
  WHERE c.custom IS NOT NULL
),

all_src AS (
  SELECT * FROM standard_src
  UNION ALL
  SELECT * FROM custom_src
),

/* Flatten objects into one row per field value */
flattened AS (
  SELECT
    s.retailer_id,
    s.dataset_id,
    s.product_id,
    f.key::STRING   AS column_name,
    f.value::STRING AS column_value
  FROM all_src s,
       LATERAL FLATTEN(input => s.src, mode => 'object') f
  WHERE f.value IS NOT NULL
    AND TRIM(f.value::STRING) <> ''
),

/* Type guessing */
typed AS (
  SELECT
    retailer_id,
    dataset_id,
    product_id,
    column_name,
    column_value,
    CASE
      WHEN column_value LIKE '%, and%' THEN 'has-comma-space-and'
      WHEN column_value LIKE '%, %'    THEN 'has-comma-space'
      WHEN column_value LIKE '%,%'     THEN 'has-comma'
      ELSE 'no-comma'
    END AS type_guess,
    LENGTH(column_value) AS value_len
  FROM flattened
),

/* Column-level stats */
col_summary AS (
  SELECT
    retailer_id,
    dataset_id,
    column_name,
    COUNT(*) AS total_rows_with_value,
    APPROX_COUNT_DISTINCT(column_value) AS approx_distinct_values
  FROM typed
  GROUP BY 1,2,3
),

/* Counts at the same grain as your current output */
guess_counts AS (
  SELECT
    retailer_id,
    dataset_id,
    column_name,
    type_guess,
    COUNT(*) AS k
  FROM typed
  GROUP BY 1,2,3,4
),

/* One sample value and product per (dataset, column, type_guess) */
sample_per_guess AS (
  SELECT
    retailer_id,
    dataset_id,
    column_name,
    type_guess,
    product_id AS sample_product_id,
    column_value AS sample_value
  FROM typed
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY retailer_id, dataset_id, column_name, type_guess
    ORDER BY value_len DESC
  ) = 1
),

/* Three longest values per (dataset, column) */
top3_longest AS (
  SELECT
    retailer_id,
    dataset_id,
    column_name,
    MAX(IFF(rn = 1, column_value, NULL)) AS longest_value_1,
    MAX(IFF(rn = 2, column_value, NULL)) AS longest_value_2,
    MAX(IFF(rn = 3, column_value, NULL)) AS longest_value_3
  FROM (
    SELECT
      retailer_id,
      dataset_id,
      column_name,
      column_value,
      ROW_NUMBER() OVER (
        PARTITION BY retailer_id, dataset_id, column_name
        ORDER BY value_len DESC
      ) AS rn
    FROM typed
  )
  WHERE rn <= 3
  GROUP BY 1,2,3
),

/* Schema lookup for dataset name and column type */
schema_cols AS (
  SELECT
    c.retailer_id,
    c.dataset_id,
    c.dataset_name,
    c.column_name,
    c.column_type
  FROM (
    /* jjp_config_dataset_column has duplicate column_names: dedupe */
    SELECT retailer_id, dataset_id, dataset_name, column_name, column_type,
        ROW_NUMBER() OVER (
        PARTITION BY retailer_id, dataset_id, column_name
        ORDER BY lower(column_name)
        ) AS rn
    FROM developer.jjp_config_dataset_column
  ) AS c
  JOIN params p
    ON c.retailer_id = p.retailer_id
    AND rn = 1
),

/* Join everything */
joined AS (
  SELECT
    r.retailer_id,
    r.retailer_name,
    gc.dataset_id,
    sc.dataset_name,
    gc.column_name,
    sc.column_type,
    gc.type_guess,
    gc.k,
    cs.total_rows_with_value,
    cs.approx_distinct_values,
    spg.sample_product_id,
    spg.sample_value,
    t3.longest_value_1,
    t3.longest_value_2,
    t3.longest_value_3
  FROM guess_counts gc
  JOIN retailer r
    ON gc.retailer_id = r.retailer_id
  LEFT JOIN schema_cols sc
    ON gc.retailer_id = sc.retailer_id
   AND gc.dataset_id  = sc.dataset_id
   AND UPPER(gc.column_name) = UPPER(sc.column_name)
  LEFT JOIN col_summary cs
    ON gc.retailer_id = cs.retailer_id
   AND gc.dataset_id  = cs.dataset_id
   AND gc.column_name = cs.column_name
  LEFT JOIN sample_per_guess spg
    ON gc.retailer_id = spg.retailer_id
   AND gc.dataset_id  = spg.dataset_id
   AND gc.column_name = spg.column_name
   AND gc.type_guess  = spg.type_guess
  LEFT JOIN top3_longest t3
    ON gc.retailer_id = t3.retailer_id
   AND gc.dataset_id  = t3.dataset_id
   AND gc.column_name = t3.column_name
),

/* Label issues so Gemini never decides */
labelled AS (
  SELECT
    'FIELD' AS record_type,
    retailer_id,
    retailer_name,
    dataset_id,
    dataset_name,
    column_name,
    column_type,
    type_guess,
    k,
    total_rows_with_value,
    approx_distinct_values,
    sample_product_id,
    sample_value,
    longest_value_1,
    longest_value_2,
    longest_value_3,

    (LENGTH(COALESCE(sample_value,'')) - LENGTH(REPLACE(COALESCE(sample_value,''), '"', ''))) AS sample_quote_count,

    IFF(
      UPPER(column_name) = 'PRODUCT_TYPE'
      AND (
        MOD((LENGTH(COALESCE(sample_value,'')) - LENGTH(REPLACE(COALESCE(sample_value,''), '"', ''))), 2) = 1
        OR (type_guess <> 'no-comma' AND (LENGTH(COALESCE(sample_value,'')) - LENGTH(REPLACE(COALESCE(sample_value,''), '"', ''))) = 0)
      ),
      1, 0
    ) AS issue_flag_product_type_escaping,

    IFF(
      UPPER(column_name) <> 'PRODUCT_TYPE'
      AND column_type IS NOT NULL
      AND UPPER(column_type) <> 'MULTI_STRING'
      AND type_guess IN ('has-comma','has-comma-space','has-comma-space-and'),
      1, 0
    ) AS issue_flag_multistring_misuse,

    CASE
      WHEN UPPER(column_name) = 'PRODUCT_TYPE' THEN 'MULTI_STRING'
      WHEN type_guess IN ('has-comma','has-comma-space','has-comma-space-and') THEN 'MULTI_STRING'
      WHEN type_guess = 'no-comma' AND UPPER(column_type) = 'MULTI_STRING' THEN 'STRING'
      ELSE COALESCE(column_type, 'UNKNOWN')
    END AS recommended_type,

    CASE
      WHEN issue_flag_product_type_escaping = 1 THEN 'HIGH'
      WHEN issue_flag_multistring_misuse = 1 THEN 'MEDIUM'
      ELSE 'NONE'
    END AS severity,

    CASE
      WHEN issue_flag_product_type_escaping = 1
        THEN 'product_type appears under-escaped for CSV parsing'
      WHEN issue_flag_multistring_misuse = 1
        THEN 'field contains comma-separated values but schema type is not MULTI_STRING'
      ELSE 'no issue detected'
    END AS issue_reason
  FROM joined
),

/* Dataset roll-up */
dataset_summary AS (
  SELECT
    'DATASET_SUMMARY' AS record_type,
    retailer_id,
    retailer_name,
    dataset_id,
    dataset_name,
    CAST(NULL AS STRING) AS column_name,
    CAST(NULL AS STRING) AS column_type,
    CAST(NULL AS STRING) AS type_guess,
    CAST(NULL AS NUMBER) AS k,
    CAST(NULL AS NUMBER) AS total_rows_with_value,
    CAST(NULL AS NUMBER) AS approx_distinct_values,
    CAST(NULL AS STRING) AS sample_product_id,
    CAST(NULL AS STRING) AS sample_value,
    CAST(NULL AS STRING) AS longest_value_1,
    CAST(NULL AS STRING) AS longest_value_2,
    CAST(NULL AS STRING) AS longest_value_3,
    CAST(NULL AS NUMBER) AS sample_quote_count,
    CAST(NULL AS NUMBER) AS issue_flag_product_type_escaping,
    CAST(NULL AS NUMBER) AS issue_flag_multistring_misuse,
    CAST(NULL AS STRING) AS recommended_type,
    CASE
      WHEN SUM(IFF(severity='HIGH',1,0)) > 0 THEN 'NOT_READY'
      WHEN SUM(IFF(severity='MEDIUM',1,0)) > 0 THEN 'READY_WITH_FIXES'
      ELSE 'READY'
    END AS severity,
    CONCAT(
      'high_issues=',
      TO_VARCHAR(SUM(IFF(severity='HIGH',1,0))),
      '; medium_issues=',
      TO_VARCHAR(SUM(IFF(severity='MEDIUM',1,0)))
    ) AS issue_reason
  FROM labelled
  GROUP BY 1,2,3,4,5
),

/* Run completeness */
run_summary AS (
  SELECT
    'RUN_SUMMARY' AS record_type,
    (SELECT retailer_id FROM params) AS retailer_id,
    (SELECT retailer_name FROM retailer) AS retailer_name,
    CAST(NULL AS NUMBER) AS dataset_id,
    CAST(NULL AS STRING) AS dataset_name,
    CAST(NULL AS STRING) AS column_name,
    CAST(NULL AS STRING) AS column_type,
    CAST(NULL AS STRING) AS type_guess,
    CAST(NULL AS NUMBER) AS k,
    CAST(NULL AS NUMBER) AS total_rows_with_value,
    CAST(NULL AS NUMBER) AS approx_distinct_values,
    CAST(NULL AS STRING) AS sample_product_id,
    CAST(NULL AS STRING) AS sample_value,
    CAST(NULL AS STRING) AS longest_value_1,
    CAST(NULL AS STRING) AS longest_value_2,
    CAST(NULL AS STRING) AS longest_value_3,
    CAST(NULL AS NUMBER) AS sample_quote_count,
    CAST(NULL AS NUMBER) AS issue_flag_product_type_escaping,
    CAST(NULL AS NUMBER) AS issue_flag_multistring_misuse,
    CAST(NULL AS STRING) AS recommended_type,
    CAST(NULL AS STRING) AS severity,
    CONCAT(
      'columns_missing_schema_type=',
      TO_VARCHAR(SUM(IFF(column_type IS NULL,1,0))),
      '; datasets_analysed=',
      TO_VARCHAR(COUNT(DISTINCT dataset_id))
    ) AS issue_reason
  FROM labelled
)

SELECT * FROM run_summary
UNION ALL
SELECT * FROM dataset_summary
UNION ALL
SELECT * FROM labelled
ORDER BY
  CASE record_type WHEN 'RUN_SUMMARY' THEN 0 WHEN 'DATASET_SUMMARY' THEN 1 ELSE 2 END,
  dataset_name,
  column_name,
  severity DESC,
  k DESC;
