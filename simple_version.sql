-- macros/redshift_safe_compare.sql
{% macro redshift_safe_compare(old_model_name, new_model_name, key_column) %}

-- Get the row counts separately to avoid correlated subqueries
WITH old_row_count AS (
  SELECT COUNT(*) as count FROM {{ ref(old_model_name) }}
),

new_row_count AS (
  SELECT COUNT(*) as count FROM {{ ref(new_model_name) }}
),

-- Use LEFT JOINs instead of NOT EXISTS
missing_keys_in_new AS (
  SELECT 
    source_model.{{ key_column }},
    1 as is_missing
  FROM {{ ref(old_model_name) }} as source_model
  LEFT JOIN {{ ref(new_model_name) }} as target_model
    ON source_model.{{ key_column }} = target_model.{{ key_column }}
  WHERE target_model.{{ key_column }} IS NULL
),

missing_keys_in_old AS (
  SELECT 
    target_model.{{ key_column }},
    1 as is_missing
  FROM {{ ref(new_model_name) }} as target_model
  LEFT JOIN {{ ref(old_model_name) }} as source_model
    ON target_model.{{ key_column }} = source_model.{{ key_column }}
  WHERE source_model.{{ key_column }} IS NULL
),

-- Count totals of missing keys
missing_counts AS (
  SELECT
    (SELECT COUNT(*) FROM missing_keys_in_new) as keys_missing_in_new,
    (SELECT COUNT(*) FROM missing_keys_in_old) as keys_missing_in_old
),

-- Calculate final summary
summary AS (
  SELECT
    '{{ old_model_name }}' as old_model,
    '{{ new_model_name }}' as new_model,
    (SELECT count FROM old_row_count) as old_record_count,
    (SELECT count FROM new_row_count) as new_record_count,
    mc.keys_missing_in_new,
    mc.keys_missing_in_old
  FROM missing_counts mc
)

SELECT * FROM summary

{% endmacro %}
