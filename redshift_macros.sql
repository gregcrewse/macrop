-- ULTRA-SIMPLE MODEL COMPARISON MACRO FOR REDSHIFT
-- This macro avoids ALL metadata functions and information_schema references
{% macro simple_compare_models(old_model_name, new_model_name, key_column) %}

-- Count records in each model
WITH old_row_count AS (
  SELECT COUNT(*) as record_count FROM {{ ref(old_model_name) }}
),

new_row_count AS (
  SELECT COUNT(*) as record_count FROM {{ ref(new_model_name) }}
),

-- Find key values in old but not in new using LEFT JOIN (Redshift friendly)
missing_in_new AS (
  SELECT COUNT(*) as count_missing
  FROM {{ ref(old_model_name) }} as source_model
  LEFT JOIN {{ ref(new_model_name) }} as target_model
    ON source_model.{{ key_column }} = target_model.{{ key_column }}
  WHERE target_model.{{ key_column }} IS NULL
),

-- Find key values in new but not in old
missing_in_old AS (
  SELECT COUNT(*) as count_missing
  FROM {{ ref(new_model_name) }} as target_model
  LEFT JOIN {{ ref(old_model_name) }} as source_model
    ON target_model.{{ key_column }} = source_model.{{ key_column }}
  WHERE source_model.{{ key_column }} IS NULL
),

-- Summary statistics
summary AS (
  SELECT
    '{{ old_model_name }}' as old_model,
    '{{ new_model_name }}' as new_model,
    (SELECT record_count FROM old_row_count) as old_record_count,
    (SELECT record_count FROM new_row_count) as new_record_count,
    (SELECT count_missing FROM missing_in_new) as records_in_old_not_in_new,
    (SELECT count_missing FROM missing_in_old) as records_in_new_not_in_old,
    ABS((SELECT record_count FROM new_row_count) - (SELECT record_count FROM old_row_count)) as record_count_difference,
    CASE 
      WHEN (SELECT record_count FROM old_row_count) = 0 THEN NULL
      ELSE ROUND(ABS((SELECT record_count FROM new_row_count) - (SELECT record_count FROM old_row_count)) * 100.0 / NULLIF((SELECT record_count FROM old_row_count), 0), 2)
    END as percentage_change
)

-- Return the summary
SELECT * FROM summary

{% endmacro %}


-- ULTRA-SIMPLE MODEL CONSOLIDATION VERIFICATION MACRO FOR REDSHIFT
{% macro verify_model_consolidation(source_models, target_model, key_column) %}
{#
  This macro verifies that a consolidation of multiple models into one target model
  retains all necessary data from the source models. Simplified for Redshift.
  
  Parameters:
    - source_models: List of models being consolidated or migrated from
    - target_model: The new or updated model receiving the consolidated data
    - key_column: Column to use as join key
#}

-- Check parameter validity
{% if source_models is string %}
  {% set source_models = [source_models] %}
{% endif %}

{% if execute %}
  {{ log('Verifying model consolidation from ' ~ source_models|join(', ') ~ ' to ' ~ target_model ~ ' using key column: ' ~ key_column, info=True) }}
  {{ log('', info=True) }}
{% endif %}

WITH 
-- Get target row count
target_count AS (
  SELECT COUNT(*) as count 
  FROM {{ ref(target_model) }}
),

-- For each source model, get row counts and missing records
{% for source_model in source_models %}
source_{{ loop.index }}_count AS (
  SELECT COUNT(*) as count 
  FROM {{ ref(source_model) }}
),

source_{{ loop.index }}_missing AS (
  SELECT COUNT(*) as count
  FROM {{ ref(source_model) }} as source
  LEFT JOIN {{ ref(target_model) }} as target 
    ON source.{{ key_column }} = target.{{ key_column }}
  WHERE target.{{ key_column }} IS NULL
),
{% endfor %}

-- Calculate source union count
union_count AS (
  SELECT COUNT(DISTINCT {{ key_column }}) as count
  FROM (
    {% for source_model in source_models %}
    SELECT {{ key_column }} FROM {{ ref(source_model) }}
    {% if not loop.last %}UNION{% endif %}
    {% endfor %}
  )
),

-- Build results
results AS (
  SELECT
    '{{ source_models | join(", ") }}' as source_models,
    '{{ target_model }}' as target_model,
    (SELECT count FROM target_count) as target_row_count,
    (SELECT count FROM union_count) as union_row_count,
    {% for source_model in source_models %}
    (SELECT count FROM source_{{ loop.index }}_count) as {{ source_model }}_row_count,
    (SELECT count FROM source_{{ loop.index }}_missing) as {{ source_model }}_missing_count,
    {% endfor %}
    CASE
      WHEN (SELECT count FROM union_count) <= (SELECT count FROM target_count) THEN 'OK'
      ELSE 'WARNING: Target has fewer rows than union of sources'
    END as validation_status
)

SELECT * FROM results

{% endmacro %}


-- ULTRA-SIMPLE DATA PROFILING MACRO FOR REDSHIFT
{% macro profile_data(model_name, columns) %}
{#
  This macro creates a basic profile of specified columns in a model.
  No metadata dependencies - you specify the columns directly.
  
  Parameters:
    - model_name: The model to profile
    - columns: List of columns to profile with their types ['col1:numeric', 'col2:string']
#}

{% set relation = ref(model_name) %}

WITH base_stats AS (
  SELECT COUNT(*) as total_rows FROM {{ relation }}
),

{% for column_info in columns %}
  {% set column_parts = column_info.split(':') %}
  {% set column = column_parts[0] %}
  {% set col_type = column_parts[1] | lower if column_parts|length > 1 else 'unknown' %}
  
  {{ column }}_stats AS (
    SELECT
      '{{ column }}' as column_name,
      '{{ col_type }}' as data_type,
      COUNT({{ column }}) as non_null_count,
      (SELECT total_rows FROM base_stats) - COUNT({{ column }}) as null_count,
      100.0 * ((SELECT total_rows FROM base_stats) - COUNT({{ column }})) / NULLIF((SELECT total_rows FROM base_stats), 0) as null_percentage
      {% if col_type in ('numeric', 'int', 'integer', 'float', 'decimal', 'number') %}
      , MIN({{ column }}) as min_value
      , MAX({{ column }}) as max_value
      , AVG({{ column }}) as avg_value
      , PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {{ column }}) as median_value
      {% elif col_type in ('string', 'varchar', 'text', 'char') %}
      , MIN(LENGTH({{ column }})) as min_length
      , MAX(LENGTH({{ column }})) as max_length
      , AVG(LENGTH({{ column }})) as avg_length
      , COUNT(DISTINCT {{ column }}) as distinct_values
      {% elif col_type in ('date', 'timestamp', 'datetime') %}
      , MIN({{ column }}) as min_date
      , MAX({{ column }}) as max_date
      , DATEDIFF('day', MIN({{ column }}), MAX({{ column }})) as date_range_days
      {% endif %}
    FROM {{ relation }}
  ){% if not loop.last %},{% endif %}
{% endfor %}

{% for column_info in columns %}
  {% set column_parts = column_info.split(':') %}
  {% set column = column_parts[0] %}
  
  SELECT * FROM {{ column }}_stats
  {% if not loop.last %}UNION ALL{% endif %}
{% endfor %}

{% endmacro %}


-- ULTRA-SIMPLE BRANCH COMPARISON MACRO FOR REDSHIFT
{% macro compare_branches_simple(model_name, schema_a, schema_b, key_column) %}
{#
  This macro compares a model across two schemas/branches using direct queries.
  No metadata dependencies.
  
  Parameters:
    - model_name: The model to compare
    - schema_a: First schema (e.g., 'dev')
    - schema_b: Second schema (e.g., 'prod')
    - key_column: Column to use as join key
#}

-- Count records in each schema
WITH schema_a_count AS (
  SELECT COUNT(*) as record_count FROM "{{ schema_a }}"."{{ model_name }}"
),

schema_b_count AS (
  SELECT COUNT(*) as record_count FROM "{{ schema_b }}"."{{ model_name }}"
),

-- Find key values in schema_a but not in schema_b
missing_in_b AS (
  SELECT COUNT(*) as count_missing
  FROM "{{ schema_a }}"."{{ model_name }}" as model_a
  LEFT JOIN "{{ schema_b }}"."{{ model_name }}" as model_b
    ON model_a.{{ key_column }} = model_b.{{ key_column }}
  WHERE model_b.{{ key_column }} IS NULL
),

-- Find key values in schema_b but not in schema_a
missing_in_a AS (
  SELECT COUNT(*) as count_missing
  FROM "{{ schema_b }}"."{{ model_name }}" as model_b
  LEFT JOIN "{{ schema_a }}"."{{ model_name }}" as model_a
    ON model_b.{{ key_column }} = model_a.{{ key_column }}
  WHERE model_a.{{ key_column }} IS NULL
),

-- Summary statistics
summary AS (
  SELECT
    '{{ model_name }}' as model_name,
    '{{ schema_a }}' as schema_a,
    '{{ schema_b }}' as schema_b,
    (SELECT record_count FROM schema_a_count) as schema_a_row_count,
    (SELECT record_count FROM schema_b_count) as schema_b_row_count,
    (SELECT count_missing FROM missing_in_b) as rows_in_a_not_in_b,
    (SELECT count_missing FROM missing_in_a) as rows_in_b_not_in_a,
    ABS((SELECT record_count FROM schema_b_count) - (SELECT record_count FROM schema_a_count)) as record_count_difference,
    CASE 
      WHEN (SELECT record_count FROM schema_a_count) = 0 THEN NULL
      ELSE ROUND(ABS((SELECT record_count FROM schema_b_count) - (SELECT record_count FROM schema_a_count)) * 100.0 / NULLIF((SELECT record_count FROM schema_a_count), 0), 2)
    END as percentage_change
)

-- Return the summary
SELECT * FROM summary

{% endmacro %}


-- ULTRA-SIMPLE DUPLICATE KEY DETECTOR FOR REDSHIFT
{% macro find_duplicates(model_name, key_columns) %}
{#
  This macro finds duplicate values for specified key columns.
  
  Parameters:
    - model_name: The model to check
    - key_columns: List of columns that should be unique ['id'] or ['first_name', 'last_name']
#}

{% set relation = ref(model_name) %}

WITH duplicate_keys AS (
  SELECT 
    {% for column in key_columns %}
    {{ column }}{% if not loop.last %}, {% endif %}
    {% endfor %},
    COUNT(*) as occurrence_count
  FROM {{ relation }}
  GROUP BY 
    {% for column in key_columns %}
    {{ column }}{% if not loop.last %}, {% endif %}
    {% endfor %}
  HAVING COUNT(*) > 1
),

summary AS (
  SELECT
    '{{ model_name }}' as model_name,
    '{{ key_columns | join(", ") }}' as key_columns,
    COUNT(*) as duplicate_key_count,
    SUM(occurrence_count) as total_duplicate_rows,
    MAX(occurrence_count) as max_occurrences
  FROM duplicate_keys
)

SELECT * FROM summary

-- If duplicates exist, also show the top 10 examples
{% if execute %}
  {% set duplicate_count_query %}
    SELECT COUNT(*) FROM duplicate_keys
  {% endset %}
  
  {% set duplicate_count = run_query(duplicate_count_query).columns[0][0] %}
  
  {% if duplicate_count > 0 %}
    UNION ALL
    
    SELECT
      'EXAMPLES' as model_name,
      {% for column in key_columns %}
      {{ column }}::VARCHAR {% if loop.first %}as key_columns{% else %}as {% endif %}{% if not loop.last %}, {% endif %}
      {% endfor %},
      occurrence_count as duplicate_key_count,
      NULL as total_duplicate_rows,
      NULL as max_occurrences
    FROM duplicate_keys
    ORDER BY occurrence_count DESC
    LIMIT 10
  {% endif %}
{% endif %}

{% endmacro %}


-- ULTRA-SIMPLE AGGREGATION COMPARISON MACRO FOR REDSHIFT
{% macro compare_aggregations(model_name, group_by_column, measure_column, aggregations=['sum', 'avg', 'count']) %}
{#
  This macro compares aggregations between different models.
  
  Parameters:
    - model_name: The model to analyze
    - group_by_column: Column to group by
    - measure_column: Column to measure
    - aggregations: List of aggregations to perform
#}

WITH data_grouped AS (
  SELECT 
    {{ group_by_column }},
    COUNT(*) as row_count,
    {% if 'sum' in aggregations %}
    SUM({{ measure_column }}) as sum_value,
    {% endif %}
    {% if 'avg' in aggregations %}
    AVG({{ measure_column }}) as avg_value,
    {% endif %}
    {% if 'min' in aggregations %}
    MIN({{ measure_column }}) as min_value,
    {% endif %}
    {% if 'max' in aggregations %}
    MAX({{ measure_column }}) as max_value,
    {% endif %}
    {% if 'stddev' in aggregations %}
    STDDEV({{ measure_column }}) as stddev_value,
    {% endif %}
    {% if 'count_distinct' in aggregations %}
    COUNT(DISTINCT {{ measure_column }}) as distinct_count,
    {% endif %}
    {% if 'median' in aggregations %}
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {{ measure_column }}) as median_value,
    {% endif %}
    COUNT(*) - COUNT({{ measure_column }}) as null_count,
    (COUNT(*) - COUNT({{ measure_column }})) * 100.0 / NULLIF(COUNT(*), 0) as null_percentage
  FROM {{ ref(model_name) }}
  GROUP BY {{ group_by_column }}
)

SELECT * FROM data_grouped
ORDER BY 
{% if 'sum' in aggregations %}
sum_value DESC
{% elif 'count' in aggregations or true %}
row_count DESC
{% endif %}

{% endmacro %}


-- ULTRA-SIMPLE NULL ANALYSIS MACRO FOR REDSHIFT
{% macro analyze_nulls(model_name, column_list=none) %}
{#
  This macro analyzes null values in a model.
  
  Parameters:
    - model_name: The model to analyze
    - column_list: Optional list of columns to analyze (if none, script will fail)
#}

{% set relation = ref(model_name) %}

WITH base_stats AS (
  SELECT COUNT(*) as total_rows FROM {{ relation }}
),

column_null_counts AS (
  SELECT
    {% for column in column_list %}
    COUNT(*) - COUNT({{ column }}) as {{ column }}_null_count,
    (COUNT(*) - COUNT({{ column }})) * 100.0 / NULLIF(COUNT(*), 0) as {{ column }}_null_percentage{% if not loop.last %},{% endif %}
    {% endfor %}
  FROM {{ relation }}
),

null_summary AS (
  SELECT
    '{{ model_name }}' as model_name,
    (SELECT total_rows FROM base_stats) as total_rows,
    {% for column in column_list %}
    '{{ column }}' as column_name,
    {{ column }}_null_count as null_count,
    {{ column }}_null_percentage as null_percentage
    {% if not loop.last %}UNION ALL
  SELECT
    '{{ model_name }}' as model_name,
    (SELECT total_rows FROM base_stats) as total_rows,
    {% endif %}
    {% endfor %}
)

SELECT * FROM null_summary
ORDER BY null_percentage DESC

{% endmacro %}


-- ULTRA-SIMPLE DATE DISTRIBUTION MACRO FOR REDSHIFT
{% macro analyze_date_distribution(model_name, date_column, date_part='month') %}
{#
  This macro analyzes the distribution of data across a date dimension.
  
  Parameters:
    - model_name: The model to analyze
    - date_column: Date column to analyze
    - date_part: Date part to group by (day, month, quarter, year)
#}

{% set relation = ref(model_name) %}

WITH date_counts AS (
  SELECT 
    {% if date_part == 'day' %}
    DATE_TRUNC('day', {{ date_column }}) as date_group,
    TO_CHAR({{ date_column }}, 'YYYY-MM-DD') as date_label
    {% elif date_part == 'month' %}
    DATE_TRUNC('month', {{ date_column }}) as date_group,
    TO_CHAR({{ date_column }}, 'YYYY-MM') as date_label
    {% elif date_part == 'quarter' %}
    DATE_TRUNC('quarter', {{ date_column }}) as date_group,
    TO_CHAR({{ date_column }}, 'YYYY-Q') as date_label
    {% elif date_part == 'year' %}
    DATE_TRUNC('year', {{ date_column }}) as date_group,
    TO_CHAR({{ date_column }}, 'YYYY') as date_label
    {% endif %},
    COUNT(*) as record_count
  FROM {{ relation }}
  WHERE {{ date_column }} IS NOT NULL
  GROUP BY date_group, date_label
),

date_stats AS (
  SELECT
    MIN(date_group) as min_date,
    MAX(date_group) as max_date,
    COUNT(*) as unique_periods,
    SUM(record_count) as total_records,
    MIN(record_count) as min_period_count,
    MAX(record_count) as max_period_count,
    AVG(record_count) as avg_period_count
  FROM date_counts
)

SELECT
  '{{ model_name }}' as model_name,
  '{{ date_column }}' as date_column,
  '{{ date_part }}' as date_granularity,
  stats.*
FROM date_stats stats

UNION ALL

SELECT
  '{{ model_name }}' as model_name,
  '{{ date_column }}' as date_column,
  date_label as date_granularity,
  NULL as min_date,
  NULL as max_date,
  NULL as unique_periods,
  record_count as total_records,
  NULL as min_period_count,
  NULL as max_period_count,
  NULL as avg_period_count
FROM date_counts
ORDER BY date_group

{% endmacro %}
