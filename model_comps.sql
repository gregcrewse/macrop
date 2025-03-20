-- in macros/comparison_macros.sql
{% macro compare_model_versions(old_model_name, new_model_name, key_column, columns_to_compare=none) %}
{# 
  This macro compares data between an old and new version of a model
  Parameters:
    - old_model_name: The name of the original model
    - new_model_name: The name of the new/updated model
    - key_column: The primary key or unique identifier column
    - columns_to_compare: Optional list of specific columns to compare (defaults to all)
#}

-- Explicitly establish relations
{% set old_relation = ref(old_model_name) %}
{% set new_relation = ref(new_model_name) %}

-- Get columns from old model
{% if columns_to_compare is none %}
  {% set old_columns_query %}
    SELECT column_name
    FROM {{ adapter.get_columns_in_relation(old_relation) }}
  {% endset %}
  
  {% set old_columns_result = run_query(old_columns_query) %}
  {% if old_columns_result and old_columns_result.columns %}
    {% set old_columns = old_columns_result.columns[0].values() %}
    {% set columns_to_compare = old_columns %}
  {% else %}
    {{ exceptions.raise_compiler_error("Could not fetch columns for model " ~ old_model_name) }}
  {% endif %}
{% endif %}

WITH old_data AS (
  SELECT 
    {{ key_column }},
    {% for column in columns_to_compare %}
      {{ column }}{% if not loop.last %},{% endif %}
    {% endfor %}
  FROM {{ old_relation }}
),

new_data AS (
  SELECT 
    {{ key_column }},
    {% for column in columns_to_compare %}
      {{ column }}{% if not loop.last %},{% endif %}
    {% endfor %}
  FROM {{ new_relation }}
),

comparison AS (
  SELECT
    COALESCE(old_data.{{ key_column }}, new_data.{{ key_column }}) AS {{ key_column }},
    {% for column in columns_to_compare %}
      {% if column != key_column %}
        CASE
          WHEN old_data.{{ column }} = new_data.{{ column }} THEN 'SAME'
          WHEN old_data.{{ column }} IS NULL AND new_data.{{ column }} IS NULL THEN 'SAME'
          WHEN old_data.{{ column }} IS NULL THEN 'NULL→VALUE'
          WHEN new_data.{{ column }} IS NULL THEN 'VALUE→NULL'
          ELSE 'DIFFERENT'
        END AS {{ column }}_comparison{% if not loop.last %},{% endif %}
      {% endif %}
    {% endfor %}
  FROM old_data
  FULL OUTER JOIN new_data
  ON old_data.{{ key_column }} = new_data.{{ key_column }}
),

summary AS (
  SELECT
    '{{ old_model_name }} vs {{ new_model_name }}' AS comparison_name,
    {% for column in columns_to_compare %}
      {% if column != key_column %}
        SUM(CASE WHEN {{ column }}_comparison = 'SAME' THEN 1 ELSE 0 END) AS {{ column }}_same_count,
        SUM(CASE WHEN {{ column }}_comparison = 'DIFFERENT' THEN 1 ELSE 0 END) AS {{ column }}_diff_count,
        SUM(CASE WHEN {{ column }}_comparison = 'NULL→VALUE' THEN 1 ELSE 0 END) AS {{ column }}_null_to_value_count,
        SUM(CASE WHEN {{ column }}_comparison = 'VALUE→NULL' THEN 1 ELSE 0 END) AS {{ column }}_value_to_null_count{% if not loop.last %},{% endif %}
      {% endif %}
    {% endfor %}
  FROM comparison
)

SELECT * FROM summary
{% endmacro %}
