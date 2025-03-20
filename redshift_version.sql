-- macros/redshift_compare_models.sql
{% macro redshift_compare_models(old_model_name, new_model_name, key_column) %}

-- Count records in each model
WITH old_count AS (
  SELECT COUNT(*) as record_count FROM {{ ref(old_model_name) }}
),

new_count AS (
  SELECT COUNT(*) as record_count FROM {{ ref(new_model_name) }}
),

-- Find records in old but not in new
missing_in_new AS (
  SELECT COUNT(*) as count_missing
  FROM {{ ref(old_model_name) }} old
  LEFT JOIN {{ ref(new_model_name) }} new
    ON old.{{ key_column }} = new.{{ key_column }}
  WHERE new.{{ key_column }} IS NULL
),

-- Find records in new but not in old
missing_in_old AS (
  SELECT COUNT(*) as count_missing
  FROM {{ ref(new_model_name) }} new
  LEFT JOIN {{ ref(old_model_name) }} old
    ON new.{{ key_column }} = old.{{ key_column }}
  WHERE old.{{ key_column }} IS NULL
),

-- Sample of missing records (for investigation)
sample_missing_in_new AS (
  SELECT old.*
  FROM {{ ref(old_model_name) }} old
  LEFT JOIN {{ ref(new_model_name) }} new
    ON old.{{ key_column }} = new.{{ key_column }}
  WHERE new.{{ key_column }} IS NULL
  LIMIT 5
),

sample_missing_in_old AS (
  SELECT new.*
  FROM {{ ref(new_model_name) }} new
  LEFT JOIN {{ ref(old_model_name) }} old
    ON new.{{ key_column }} = old.{{ key_column }}
  WHERE old.{{ key_column }} IS NULL
  LIMIT 5
),

-- Summary statistics
summary AS (
  SELECT
    '{{ old_model_name }}' as old_model,
    '{{ new_model_name }}' as new_model,
    (SELECT record_count FROM old_count) as old_record_count,
    (SELECT record_count FROM new_count) as new_record_count,
    (SELECT count_missing FROM missing_in_new) as records_in_old_not_in_new,
    (SELECT count_missing FROM missing_in_old) as records_in_new_not_in_old,
    ABS((SELECT record_count FROM new_count) - (SELECT record_count FROM old_count)) as record_count_difference,
    CASE 
      WHEN (SELECT record_count FROM old_count) = 0 THEN NULL
      ELSE ROUND(ABS((SELECT record_count FROM new_count) - (SELECT record_count FROM old_count)) * 100.0 / NULLIF((SELECT record_count FROM old_count), 0), 2)
    END as percentage_change
)

-- Return the summary
SELECT * FROM summary

{% endmacro %}
