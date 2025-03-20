-- macros/simple_compare_models.sql
{% macro simple_compare_models(old_model_name, new_model_name, key_column) %}

-- Count records in each model
WITH old_count AS (
  SELECT COUNT(*) as record_count FROM {{ ref(old_model_name) }}
),

new_count AS (
  SELECT COUNT(*) as record_count FROM {{ ref(new_model_name) }}
),

-- Find key values in old but not in new
missing_in_new AS (
  SELECT {{ key_column }}, COUNT(*) as count
  FROM {{ ref(old_model_name) }} old
  WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ ref(new_model_name) }} new
    WHERE new.{{ key_column }} = old.{{ key_column }}
  )
  GROUP BY {{ key_column }}
),

-- Find key values in new but not in old
missing_in_old AS (
  SELECT {{ key_column }}, COUNT(*) as count
  FROM {{ ref(new_model_name) }} new
  WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ ref(old_model_name) }} old
    WHERE old.{{ key_column }} = new.{{ key_column }}
  )
  GROUP BY {{ key_column }}
),

-- Summary statistics
summary AS (
  SELECT
    '{{ old_model_name }}' as old_model,
    '{{ new_model_name }}' as new_model,
    (SELECT record_count FROM old_count) as old_record_count,
    (SELECT record_count FROM new_count) as new_record_count,
    (SELECT COUNT(*) FROM missing_in_new) as keys_missing_in_new,
    (SELECT COUNT(*) FROM missing_in_old) as keys_missing_in_old
)

-- Return the summary
SELECT * FROM summary

{% endmacro %}
