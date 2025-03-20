-- 1. DATA COMPARISON MACRO
{% macro compare_model_versions(old_model_name, new_model_name, key_column, columns_to_compare=none) %}
{# 
  This macro compares data between an old and new version of a model
  Parameters:
    - old_model_name: The name of the original model
    - new_model_name: The name of the new/updated model
    - key_column: The primary key or unique identifier column
    - columns_to_compare: Optional list of specific columns to compare (defaults to all)
#}

{% set old_relation = ref(old_model_name) %}
{% set new_relation = ref(new_model_name) %}

{% if columns_to_compare is none %}
  {% set old_columns_query %}
    SELECT column_name
    FROM {{ information_schema_columns(old_relation) }}
    ORDER BY ordinal_position
  {% endset %}
  
  {% set old_columns = run_query(old_columns_query).columns[0].values() %}
  {% set columns_to_compare = old_columns %}
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


-- 2. DATA SUMMARY MACRO
{% macro summarize_model(model_name, group_by_columns=none, numeric_columns=none) %}
{#
  This macro generates summary statistics for a given model
  Parameters:
    - model_name: The name of the model to summarize
    - group_by_columns: Optional list of columns to group by
    - numeric_columns: Optional list of numeric columns to summarize
#}

{% set relation = ref(model_name) %}

{% if numeric_columns is none %}
  {% set numeric_columns_query %}
    SELECT column_name 
    FROM {{ information_schema_columns(relation) }}
    WHERE data_type IN ('integer', 'number', 'float', 'decimal', 'double', 'real', 'numeric')
    ORDER BY ordinal_position
  {% endset %}
  
  {% set numeric_columns = run_query(numeric_columns_query).columns[0].values() %}
{% endif %}

{% if group_by_columns is none %}
  -- Global summary without grouping
  SELECT
    '{{ model_name }}' AS model_name,
    COUNT(*) AS total_rows,
    {% for column in numeric_columns %}
      MIN({{ column }}) AS {{ column }}_min,
      MAX({{ column }}) AS {{ column }}_max,
      AVG({{ column }}) AS {{ column }}_avg,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {{ column }}) AS {{ column }}_median,
      COUNT({{ column }}) AS {{ column }}_count,
      COUNT(*) - COUNT({{ column }}) AS {{ column }}_null_count,
      (COUNT(*) - COUNT({{ column }})) * 100.0 / COUNT(*) AS {{ column }}_null_percentage
      {% if not loop.last %},{% endif %}
    {% endfor %}
  FROM {{ relation }}
{% else %}
  -- Grouped summary
  SELECT
    {% for column in group_by_columns %}
      {{ column }},
    {% endfor %}
    COUNT(*) AS total_rows,
    {% for column in numeric_columns %}
      {% if column not in group_by_columns %}
        MIN({{ column }}) AS {{ column }}_min,
        MAX({{ column }}) AS {{ column }}_max,
        AVG({{ column }}) AS {{ column }}_avg,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {{ column }}) AS {{ column }}_median,
        COUNT({{ column }}) AS {{ column }}_count,
        COUNT(*) - COUNT({{ column }}) AS {{ column }}_null_count,
        (COUNT(*) - COUNT({{ column }})) * 100.0 / COUNT(*) AS {{ column }}_null_percentage
        {% if not loop.last %},{% endif %}
      {% endif %}
    {% endfor %}
  FROM {{ relation }}
  GROUP BY
    {% for column in group_by_columns %}
      {{ column }}{% if not loop.last %},{% endif %}
    {% endfor %}
{% endif %}

{% endmacro %}


-- 3. MODEL REFERENCE DISCOVERY AND UPDATE
{% macro find_model_references(old_model_name) %}
{#
  This macro finds all references to a given model in your project
  Parameters:
    - old_model_name: The name of the model to find references for
#}

{% set project_files_query %}
  SELECT 
    f.path as file_path
  FROM {{ information_schema.files }} f
  WHERE f.path LIKE '%.sql'
{% endset %}

{% set project_files = run_query(project_files_query).columns[0].values() %}

{% set results = [] %}
{% for file_path in project_files %}
  {% set file_content_query %}
    SELECT file_content 
    FROM {{ information_schema.file_content(file_path) }}
  {% endset %}
  
  {% set file_content = run_query(file_content_query).columns[0][0] %}
  
  {% if file_content and file_content is string and file_content | lower is contains('ref(\'' ~ old_model_name ~ '\')') or file_content | lower is contains('ref("' ~ old_model_name ~ '")') %}
    {% do results.append(file_path) %}
  {% endif %}
{% endfor %}

{% if execute %}
  {{ log('Found references to model "' ~ old_model_name ~ '" in the following files:', info=True) }}
  {% for file in results %}
    {{ log('  - ' ~ file, info=True) }}
  {% endfor %}
{% endif %}

{% endmacro %}


-- 4. MODEL REPLACEMENT SCRIPT (Python script to be saved in dbt_project/scripts/)
-- This is a Python script, not a SQL macro
/*
#!/usr/bin/env python3
"""
Model Replacement Utility for dbt

This script scans SQL files in a dbt project and replaces references to an old model
with references to a new model name.

Usage:
  python replace_model_references.py --old-model old_model_name --new-model new_model_name [--project-dir ./]

Examples:
  python replace_model_references.py --old-model stg_customers --new-model stg_customers_v2
"""

import argparse
import os
import re
import sys
from pathlib import Path


def find_and_replace_references(old_model, new_model, project_dir='.'):
    """Find all references to old_model and replace with new_model."""
    # Regular expressions to match ref() function calls with both single and double quotes
    ref_pattern_single = re.compile(r"ref\(\s*'(" + re.escape(old_model) + r")'\s*\)")
    ref_pattern_double = re.compile(r'ref\(\s*"(' + re.escape(old_model) + r')"\s*\)')
    
    # Find all SQL files in the project
    sql_files = []
    for root, _, files in os.walk(project_dir):
        for file in files:
            if file.endswith('.sql'):
                sql_files.append(os.path.join(root, file))
    
    modified_files = []
    for file_path in sql_files:
        try:
            with open(file_path, 'r') as file:
                content = file.read()
            
            # Check if the file contains references to the old model
            if re.search(ref_pattern_single, content) or re.search(ref_pattern_double, content):
                # Replace references
                new_content = re.sub(ref_pattern_single, f"ref('{new_model}')", content)
                new_content = re.sub(ref_pattern_double, f'ref("{new_model}")', new_content)
                
                # Save the modified content
                with open(file_path, 'w') as file:
                    file.write(new_content)
                
                modified_files.append(file_path)
        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")
    
    return modified_files


def main():
    parser = argparse.ArgumentParser(description='Replace model references in dbt project')
    parser.add_argument('--old-model', required=True, help='Name of the model to be replaced')
    parser.add_argument('--new-model', required=True, help='Name of the new model')
    parser.add_argument('--project-dir', default='.', help='Path to dbt project directory (default: current directory)')
    
    args = parser.parse_args()
    
    if args.old_model == args.new_model:
        print("Error: Old model name and new model name are the same.")
        sys.exit(1)
    
    print(f"Replacing references to '{args.old_model}' with '{args.new_model}'...")
    modified_files = find_and_replace_references(args.old_model, args.new_model, args.project_dir)
    
    if modified_files:
        print(f"\nModified {len(modified_files)} files:")
        for file_path in modified_files:
            print(f"  - {file_path}")
    else:
        print(f"\nNo references to '{args.old_model}' found in SQL files.")


if __name__ == "__main__":
    main()
*/


-- 5. MODEL CHANGE TESTING MACRO
{% macro test_model_change(old_model_name, new_model_name, config_options={}) %}
{#
  This macro creates a test to verify a new model against an old one
  Parameters:
    - old_model_name: The name of the original model
    - new_model_name: The name of the new/updated model
    - config_options: Optional configuration options for the test
#}

{{ config(options=config_options) }}

-- Count checks
WITH old_model_count AS (
  SELECT COUNT(*) AS row_count FROM {{ ref(old_model_name) }}
),

new_model_count AS (
  SELECT COUNT(*) AS row_count FROM {{ ref(new_model_name) }}
),

count_check AS (
  SELECT 
    old_model_count.row_count AS old_row_count,
    new_model_count.row_count AS new_row_count,
    ABS(new_model_count.row_count - old_model_count.row_count) AS row_count_diff,
    CASE 
      WHEN old_model_count.row_count = 0 THEN NULL
      ELSE ABS(new_model_count.row_count - old_model_count.row_count) / old_model_count.row_count
    END AS row_count_pct_change
  FROM old_model_count, new_model_count
),

-- Fail the test if count change exceeds threshold
test_result AS (
  SELECT
    CASE 
      WHEN row_count_pct_change > 0.1 THEN 'FAIL: Row count changed by more than 10%'
      ELSE 'PASS'
    END AS status,
    old_row_count,
    new_row_count,
    row_count_diff,
    row_count_pct_change
  FROM count_check
  WHERE status = 'FAIL: Row count changed by more than 10%'
)

SELECT * FROM test_result

{% endmacro %}
