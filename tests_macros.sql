-- 1. REDSHIFT-COMPATIBLE DATA COMPARISON MACRO
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
  -- Get columns directly from the database using Redshift-friendly approach
  {% set old_columns_query %}
    SELECT column_name
    FROM pg_table_def
    WHERE tablename = '{{ old_model_name }}' 
    AND schemaname = '{{ old_relation.schema }}'
    ORDER BY ordinal_position
  {% endset %}
  
  {% set old_columns_result = run_query(old_columns_query) %}
  {% if execute and old_columns_result and old_columns_result.rows %}
    {% set old_columns = old_columns_result.columns[0].values() %}
    {% set columns_to_compare = old_columns %}
  {% else %}
    -- Fallback to a few basic columns if metadata query fails
    {% set columns_to_compare = [key_column] %}
    {{ log("WARNING: Could not fetch columns for model " ~ old_model_name ~ ". Using key column only.", info=true) }}
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
    COALESCE(source_model.{{ key_column }}, target_model.{{ key_column }}) AS {{ key_column }},
    {% for column in columns_to_compare %}
      {% if column != key_column %}
        CASE
          WHEN source_model.{{ column }} = target_model.{{ column }} THEN 'SAME'
          WHEN source_model.{{ column }} IS NULL AND target_model.{{ column }} IS NULL THEN 'SAME'
          WHEN source_model.{{ column }} IS NULL THEN 'NULL→VALUE'
          WHEN target_model.{{ column }} IS NULL THEN 'VALUE→NULL'
          ELSE 'DIFFERENT'
        END AS {{ column }}_comparison{% if not loop.last %},{% endif %}
      {% endif %}
    {% endfor %}
  FROM old_data AS source_model
  FULL OUTER JOIN new_data AS target_model
  ON source_model.{{ key_column }} = target_model.{{ key_column }}
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


-- 2. REDSHIFT-COMPATIBLE DATA SUMMARY MACRO
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
    FROM pg_table_def
    WHERE tablename = '{{ model_name }}'
    AND schemaname = '{{ relation.schema }}'
    AND type IN ('int', 'integer', 'bigint', 'smallint', 'numeric', 'decimal', 'double precision', 'real')
    ORDER BY ordinal_position
  {% endset %}
  
  {% set numeric_columns_result = run_query(numeric_columns_query) %}
  {% if execute and numeric_columns_result and numeric_columns_result.rows %}
    {% set numeric_columns = numeric_columns_result.columns[0].values() %}
  {% else %}
    {% set numeric_columns = [] %}
    {{ log("WARNING: Could not fetch numeric columns for model " ~ model_name ~ ".", info=true) }}
  {% endif %}
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
      (COUNT(*) - COUNT({{ column }})) * 100.0 / NULLIF(COUNT(*), 0) AS {{ column }}_null_percentage
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
        (COUNT(*) - COUNT({{ column }})) * 100.0 / NULLIF(COUNT(*), 0) AS {{ column }}_null_percentage
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


-- 3. REDSHIFT-COMPATIBLE MODEL REFERENCE DISCOVERY AND UPDATE
{% macro find_model_references(old_model_name) %}
{#
  This macro finds all references to a given model in your project
  Parameters:
    - old_model_name: The name of the model to find references for
#}

-- For Redshift, this is best done by scanning files outside the database
-- This is a simplified version that logs direct references

{% set project_files_query %}
  SELECT f.path
  FROM (
    SELECT path
    FROM {{ ref('dbt_project_files') }}
    WHERE path LIKE '%.sql'
  ) AS f
{% endset %}

{% set project_files_result = run_query(project_files_query) %}
{% set results = [] %}
{% if execute and project_files_result and project_files_result.rows %}
  {% set project_files = project_files_result.columns[0].values() %}
  
  {% for file_path in project_files %}
    {% set file_content_query %}
      SELECT file_content 
      FROM {{ ref('dbt_file_contents') }}
      WHERE file_path = '{{ file_path }}'
    {% endset %}
    
    {% set file_content_result = run_query(file_content_query) %}
    {% if file_content_result and file_content_result.rows %}
      {% set file_content = file_content_result.columns[0][0] %}
      
      {% if file_content and file_content is string and file_content | lower is contains('ref(\'' ~ old_model_name ~ '\')') or file_content | lower is contains('ref("' ~ old_model_name ~ '")') %}
        {% do results.append(file_path) %}
      {% endif %}
    {% endif %}
  {% endfor %}
  
  {{ log('Found references to model "' ~ old_model_name ~ '" in the following files:', info=True) }}
  {% for file in results %}
    {{ log('  - ' ~ file, info=True) }}
  {% endfor %}
{% else %}
  {{ log('WARNING: Could not find project files. Make sure you have a dbt_project_files seed file.', info=True) }}
  {{ log('To find model references, you may need to use a shell script or Python script instead.', info=True) }}
{% endif %}

{% endmacro %}


-- 4. REDSHIFT-COMPATIBLE MODEL CHANGE TESTING MACRO
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
      ELSE ABS(new_model_count.row_count - old_model_count.row_count) / NULLIF(old_model_count.row_count, 0)
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


-- 5. REDSHIFT-COMPATIBLE COLUMN LINEAGE TRACKER
{% macro trace_column_lineage(model_name, column_name) %}
{#
  This macro traces the lineage of a specific column through upstream models
  Parameters:
    - model_name: The model containing the column
    - column_name: The column to trace
#}

-- For Redshift, this needs to use dbt's graph objects rather than information_schema
{% set model_refs_query %}
  WITH RECURSIVE model_lineage AS (
    -- Base case: the starting model
    SELECT 
      node_id AS unique_id,
      model_name AS name,
      NULL AS referenced_by,
      0 AS depth
    FROM {{ ref('dbt_models') }}
    WHERE model_name = '{{ model_name }}'
    
    UNION ALL
    
    -- Recursive case: all upstream models
    SELECT
      upstream.node_id AS unique_id,
      upstream.model_name AS name,
      ml.name AS referenced_by,
      ml.depth + 1 AS depth
    FROM model_lineage ml
    JOIN {{ ref('dbt_dependencies') }} deps ON deps.downstream_id = ml.unique_id
    JOIN {{ ref('dbt_models') }} upstream ON upstream.node_id = deps.upstream_id
  )
  
  SELECT
    unique_id,
    name,
    referenced_by,
    depth
  FROM model_lineage
  ORDER BY depth ASC, name
{% endset %}

{% set model_refs_result = run_query(model_refs_query) %}

{% if execute and model_refs_result and model_refs_result.rows %}
  {{ log('Tracing lineage for column "' ~ column_name ~ '" in model "' ~ model_name ~ '":', info=True) }}
  
  {% for model_ref in model_refs_result %}
    {% set current_model = model_ref['name'] %}
    {% set depth = model_ref['depth'] %}
    {% set referenced_by = model_ref['referenced_by'] %}
    
    {% set model_sql_query %}
      SELECT model_sql 
      FROM {{ ref('dbt_models') }}
      WHERE model_name = '{{ current_model }}'
    {% endset %}
    
    {% set model_sql_result = run_query(model_sql_query) %}
    
    {% if model_sql_result and model_sql_result.rows %}
      {% set model_sql = model_sql_result.columns[0][0] %}
      
      {% if model_sql and model_sql is string %}
        {% set indentation = '  ' * depth %}
        {% if column_name | lower in model_sql | lower %}
          {{ log(indentation ~ '- ' ~ current_model ~ (referenced_by is not none and ' (referenced by ' ~ referenced_by ~ ')' or ''), info=True) }}
          
          -- Extract column references in SELECT statements
          {% set select_pattern = r'SELECT.*?FROM'|regex_search(model_sql, ignorecase=True) %}
          {% if select_pattern %}
            {% set select_clause = select_pattern[0] %}
            {% if column_name | lower in select_clause | lower %}
              -- Found the column in SELECT clause, look for its definition
              {{ log(indentation ~ '  Column "' ~ column_name ~ '" found in SELECT clause', info=True) }}
            {% endif %}
          {% endif %}
        {% endif %}
      {% endif %}
    {% endif %}
  {% endfor %}
{% else %}
  {{ log('WARNING: Could not find model lineage data. Make sure you have dbt_models and dbt_dependencies seed files.', info=True) }}
{% endif %}

{% endmacro %}


-- 6. REDSHIFT-COMPATIBLE MODEL DEPENDENCY VISUALIZER
{% macro visualize_model_dependencies(model_name, max_depth=3) %}
{#
  This macro generates a Mermaid diagram of model dependencies
  Parameters:
    - model_name: The model to visualize dependencies for
    - max_depth: Maximum depth of upstream dependencies to show
#}

{% set dependencies_query %}
  WITH RECURSIVE model_deps AS (
    -- Base case: the starting model
    SELECT 
      node_id AS unique_id,
      model_name AS name,
      'model' AS resource_type,
      0 AS depth
    FROM {{ ref('dbt_models') }}
    WHERE model_name = '{{ model_name }}'
    
    UNION ALL
    
    -- Recursive case: upstream dependencies
    SELECT
      upstream.node_id AS unique_id,
      upstream.model_name AS name,
      'model' AS resource_type,
      d.depth + 1 AS depth
    FROM model_deps d
    JOIN {{ ref('dbt_dependencies') }} deps ON deps.downstream_id = d.unique_id
    JOIN {{ ref('dbt_models') }} upstream ON upstream.node_id = deps.upstream_id
    WHERE d.depth < {{ max_depth }}
  )
  
  SELECT DISTINCT
    unique_id,
    name,
    resource_type,
    depth
  FROM model_deps
  ORDER BY depth ASC, name
{% endset %}

{% set dependencies_result = run_query(dependencies_query) %}

{% set edges_query %}
  WITH model_deps AS (
    SELECT 
      node_id AS unique_id,
      model_name AS name
    FROM {{ ref('dbt_models') }}
    WHERE model_name IN (
      SELECT name FROM ({{ dependencies_query }})
    )
  )
  
  SELECT DISTINCT
    source_model.name AS source_name,
    target_model.name AS target_name
  FROM {{ ref('dbt_dependencies') }} deps
  JOIN model_deps source_model ON source_model.unique_id = deps.upstream_id
  JOIN model_deps target_model ON target_model.unique_id = deps.downstream_id
  ORDER BY source_name, target_name
{% endset %}

{% set edges_result = run_query(edges_query) %}

{% if execute and dependencies_result and dependencies_result.rows %}
  {{ log('graph TD', info=True) }}
  
  -- Define nodes with styling
  {% for dep in dependencies_result %}
    {% set node_name = dep['name'] %}
    {% set resource_type = dep['resource_type'] %}
    {% set depth = dep['depth'] %}
    
    {% set node_style = '' %}
    {% if node_name == model_name %}
      {% set node_style = ':::focus' %}
    {% elif resource_type == 'model' %}
      {% set node_style = ':::model' %}
    {% elif resource_type == 'source' %}
      {% set node_style = ':::source' %}
    {% elif resource_type == 'seed' %}
      {% set node_style = ':::seed' %}
    {% endif %}
    
    {{ log('  ' ~ node_name ~ node_style, info=True) }}
  {% endfor %}
  
  -- Define edges
  {% if edges_result and edges_result.rows %}
    {% for edge in edges_result %}
      {{ log('  ' ~ edge['source_name'] ~ ' --> ' ~ edge['target_name'], info=True) }}
    {% endfor %}
  {% endif %}
  
  -- Define styles
  {{ log('  classDef focus fill:#f96,stroke:#333,stroke-width:2px;', info=True) }}
  {{ log('  classDef model fill:#bbf,stroke:#33f,stroke-width:1px;', info=True) }}
  {{ log('  classDef source fill:#bfb,stroke:#3f3,stroke-width:1px;', info=True) }}
  {{ log('  classDef seed fill:#fbf,stroke:#f3f,stroke-width:1px;', info=True) }}
{% else %}
  {{ log('WARNING: Could not find model dependency data. Make sure you have dbt_models and dbt_dependencies seed files.', info=True) }}
{% endif %}

{% endmacro %}


-- 7. REDSHIFT-COMPATIBLE DATA QUALITY MONITORING MACRO
{% macro monitor_data_quality(model_name, tests_to_run=['not_null', 'unique', 'relationships'], columns=none) %}
{#
  This macro generates data quality tests for specified columns
  Parameters:
    - model_name: The model to generate tests for
    - tests_to_run: List of test types to generate
    - columns: Optional list of columns to test (defaults to all)
#}

{% set relation = ref(model_name) %}

{% if columns is none %}
  {% set columns_query %}
    SELECT column_name
    FROM pg_table_def
    WHERE tablename = '{{ model_name }}'
    AND schemaname = '{{ relation.schema }}'
    ORDER BY ordinal_position
  {% endset %}
  
  {% set columns_result = run_query(columns_query) %}
  {% if execute and columns_result and columns_result.rows %}
    {% set columns = columns_result.columns[0].values() %}
  {% else %}
    {% set columns = [] %}
    {{ log("WARNING: Could not fetch columns for model " ~ model_name ~ ".", info=true) }}
  {% endif %}
{% endif %}

{% if execute %}
  {{ log('# Data Quality Tests for model: ' ~ model_name, info=True) }}
  
  {% for column in columns %}
    {{ log('version: 2', info=True) }}
    {{ log('', info=True) }}
    {{ log('models:', info=True) }}
    {{ log('  - name: ' ~ model_name, info=True) }}
    {{ log('    columns:', info=True) }}
    {{ log('      - name: ' ~ column, info=True) }}
    {{ log('        tests:', info=True) }}
    
    {% if 'not_null' in tests_to_run %}
      {{ log('          - not_null', info=True) }}
    {% endif %}
    
    {% if 'unique' in tests_to_run and loop.index == 1 %}
      -- Assume first column might be a primary key
      {{ log('          - unique', info=True) }}
    {% endif %}
    
    {% if 'relationships' in tests_to_run and column | lower is containing('_id') %}
      -- For columns that look like foreign keys
      {{ log('          - relationships:', info=True) }}
      {{ log('              to: ref(\'' ~ column | replace('_id', '') ~ '\')', info=True) }}
      {{ log('              field: id', info=True) }}
    {% endif %}
    
    {% if 'accepted_values' in tests_to_run %}
      -- Get possible values for the column (limited to prevent huge lists)
      {% set values_query %}
        SELECT DISTINCT {{ column }}
        FROM {{ relation }}
        WHERE {{ column }} IS NOT NULL
        LIMIT 10
      {% endset %}
      
      {% set values_result = run_query(values_query) %}
      {% if values_result and values_result.rows and values_result.rows | length > 0 and values_result.rows | length < 10 %}
        {% set values = values_result.columns[0].values() %}
        {{ log('          - accepted_values:', info=True) }}
        {{ log('              values: [' ~ values|join(', ') ~ ']', info=True) }}
      {% endif %}
    {% endif %}
    
    {{ log('', info=True) }}
  {% endfor %}
{% endif %}

{% endmacro %}


-- 8. REDSHIFT-COMPATIBLE INCREMENTAL TESTING MACRO
{% macro test_incremental_load(model_name, date_column, lookback_days=7) %}
{#
  This macro tests incremental loading for a model
  Parameters:
    - model_name: The model to test incremental loading for
    - date_column: The column used for incremental filtering
    - lookback_days: Number of days to test
#}

-- Get the max date in the model
{% set max_date_query %}
  SELECT MAX({{ date_column }}) as max_date
  FROM {{ ref(model_name) }}
{% endset %}

{% set max_date_result = run_query(max_date_query) %}
{% if execute and max_date_result and max_date_result.rows %}
  {% set max_date = max_date_result.columns[0][0] %}

  -- Create a CTE for each day in the lookback period
  WITH 
  {% for i in range(lookback_days) %}
    day_{{ i }} AS (
      SELECT 
        '{{ model_name }}' as model_name,
        DATEADD(day, -{{ i }}, '{{ max_date }}') as load_date,
        COUNT(*) as record_count
      FROM {{ ref(model_name) }}
      WHERE {{ date_column }} <= DATEADD(day, -{{ i }}, '{{ max_date }}')
    ){% if not loop.last %},{% endif %}
  {% endfor %}

  -- Union all day CTEs
  SELECT * FROM 
  {% for i in range(lookback_days) %}
    day_{{ i }}{% if not loop.last %} UNION ALL {% endif %}
  {% endfor %}
  ORDER BY load_date DESC
{% else %}
  -- Return a placeholder result if unable to get max date
  SELECT
    '{{ model_name }}' as model_name,
    CURRENT_DATE as load_date,
    0 as record_count
{% endif %}

{% endmacro %}


-- 9. REDSHIFT-COMPATIBLE SCHEMA CHANGE DETECTOR
{% macro detect_schema_changes(model_name, compare_env='prod') %}
{#
  This macro detects schema changes between environments
  Parameters:
    - model_name: The model to check for schema changes
    - compare_env: The environment to compare with (default: prod)
#}

-- Get current environment schema
{% set current_schema_query %}
  SELECT 
    column_name,
    type as data_type,
    character_maximum_length,
    CASE WHEN is_nullable = 'YES' THEN TRUE ELSE FALSE END as is_nullable
  FROM pg_table_def
  WHERE tablename = '{{ model_name }}'
  AND schemaname = '{{ target.schema }}'
  ORDER BY ordinal_position
{% endset %}

{% set current_schema_result = run_query(current_schema_query) %}

-- Get comparison environment schema reference
{% set prod_schema_query %}
  SELECT 
    column_name,
    type as data_type,
    character_maximum_length,
    CASE WHEN is_nullable = 'YES' THEN TRUE ELSE FALSE END as is_nullable
  FROM pg_table_def
  WHERE tablename = '{{ model_name }}'
  AND schemaname = '{{ env_var(compare_env ~ "_schema", compare_env) }}'
  ORDER BY ordinal_position
{% endset %}

{% if execute %}
  -- Try to run the comparison query, but it might fail if env doesn't exist
  {% try %}
    {% set prod_schema_result = run_query(prod_schema_query) %}
    
    {% if current_schema_result and current_schema_result.rows and prod_schema_result and prod_schema_result.rows %}
      -- Convert result sets to dictionaries for comparison
      {% set current_columns = {} %}
      {% for row in current_schema_result %}
        {% do current_columns.update({row['column_name']: {
          'data_type': row['data_type'],
          'character_maximum_length': row['character_maximum_length'],
          'is_nullable': row['is_nullable']
        }}) %}
      {% endfor %}
      
      {% set prod_columns = {} %}
      {% for row in prod_schema_result %}
        {% do prod_columns.update({row['column_name']: {
          'data_type': row['data_type'],
          'character_maximum_length': row['character_maximum_length'],
          'is_nullable': row['is_nullable']
        }}) %}
      {% endfor %}
      
      -- Find columns in current but not in prod (added)
      {% set added_columns = [] %}
      {% for col in current_columns.keys() %}
        {% if col not in prod_columns %}
          {% do added_columns.append(col) %}
        {% endif %}
      {% endfor %}
      
      -- Find columns in prod but not in current (removed)
      {% set removed_columns = [] %}
      {% for col in prod_columns.keys() %}
        {% if col not in current_columns %}
          {% do removed_columns.append(col) %}
        {% endif %}
      {% endfor %}
      
      -- Find columns with type changes
      {% set changed_columns = [] %}
      {% for col in current_columns.keys() %}
        {% if col in prod_columns %}
          {% set current = current_columns[col] %}
          {% set prod = prod_columns[col] %}
          {% if current.data_type != prod.data_type or 
                current.character_maximum_length != prod.character_maximum_length or
                current.is_nullable != prod.is_nullable %}
            {% do changed_columns.append({
              'name': col,
              'current_type': current.data_type,
              'prod_type': prod.data_type,
              'current_length': current.character_maximum_length,
              'prod_length': prod.character_maximum_length,
              'current_nullable': current.is_nullable,
              'prod_nullable': prod.is_nullable
            }) %}
          {% endif %}
        {% endif %}
      {% endfor %}
      
      -- Log the results
      {{ log('Schema change detection for model "' ~ model_name ~ '" comparing with ' ~ compare_env ~ ':', info=True) }}
      
      {% if added_columns %}
        {{ log('Added columns:', info=True) }}
        {% for col in added_columns %}
          {{ log('  - ' ~ col ~ ' (' ~ current_columns[col].data_type ~ ')', info=True) }}
        {% endfor %}
      {% else %}
        {{ log('No columns added.', info=True) }}
      {% endif %}
      
      {% if removed_columns %}
        {{ log('Removed columns:', info=True) }}
        {% for col in removed_columns %}
          {{ log('  - ' ~ col ~ ' (' ~ prod_columns[col].data_type ~ ')', info=True) }}
        {% endfor %}
      {% else %}
        {{ log('No columns removed.', info=True) }}
      {% endif %}
      
      {% if changed_columns %}
        {{ log('Changed columns:', info=True) }}
        {% for col in changed_columns %}
          {% set changes = [] %}
          {% if col.current_type != col.prod_type %}
            {% do changes.append('type: ' ~ col.prod_type ~ ' → ' ~ col.current_type) %}
          {% endif %}
          {% if col.current_length != col.prod_length %}
            {% do changes.append('length: ' ~ col.prod_length ~ ' → ' ~ col.current_length) %}
          {% endif %}
          {% if col.current_nullable != col.prod_nullable %}
            {% do changes.append('nullable: ' ~ col.prod_nullable ~ ' → ' ~ col.current_nullable) %}
          {% endif %}
          {{ log('  - ' ~ col.name ~ ' (' ~ changes|join(', ') ~ ')', info=True) }}
        {% endfor %}
      {% else %}
        {{ log('No columns changed.', info=True) }}
      {% endif %}
    {% else %}
      {{ log('WARNING: Could not fetch schema information from one or both environments.', info=True) }}
    {% endif %}
  {% endtry %}
{% endif %}

{% endmacro %}


-- 10. REDSHIFT-COMPATIBLE MODEL CONSOLIDATION VERIFICATION MACRO
{% macro verify_model_consolidation(source_models, target_model, key_columns=none, comparison_type='full') %}
{#
  This macro verifies that a consolidation of multiple models into one target model
  retains all necessary data from the source models.
  
  Parameters:
    - source_models: List of models being consolidated or migrated from
    - target_model: The new or updated model receiving the consolidated data
    - key_columns: List of columns to use as join keys (if null, will attempt to find common keys)
    - comparison_type: Type of comparison to perform ('full', 'union', 'rows', 'schema')
#}

-- Check parameter validity
{% if source_models is string %}
  {% set source_models = [source_models] %}
{% endif %}

{% if execute %}
  {{ log('Verifying model consolidation from ' ~ source_models|join(', ') ~ ' to ' ~ target_model, info=True) }}
  {{ log('', info=True) }}
{% endif %}

-- Get the target model relation
{% set target_relation = ref(target_model) %}

-- Get target model columns using Redshift's pg_table_def
{% set target_columns_query %}
  SELECT 
    column_name,
    type as data_type,
    CASE WHEN is_nullable = 'YES' THEN TRUE ELSE FALSE END as is_nullable
  FROM pg_table_def
  WHERE tablename = '{{ target_model }}'
  AND schemaname = '{{ target_relation.schema }}'
  ORDER BY ordinal_position
{% endset %}

{% set target_columns_result = run_query(target_columns_query) %}

-- Store target model columns in a dictionary for easy lookup
{% set target_cols = {} %}
{% if execute and target_columns_result and target_columns_result.rows %}
  {% for col in target_columns_result %}
    {% do target_cols.update({col['column_name']: {
      'data_type': col['data_type'],
      'is_nullable': col['is_nullable']
    }}) %}
  {% endfor %}
{% endif %}

-- Get target row count
{% set target_count_query %}
  SELECT COUNT(*) FROM {{ target_relation }}
{% endset %}

{% set target_count_result = run_query(target_count_query) %}
{% set target_count = target_count_result.columns[0][0] if target_count_result and target_count_result.rows else 0 %}

-- Store source model info
{% set source_models_info = [] %}

-- Process each source model
{% for source_model in source_models %}
  {% set source_relation = ref(source_model) %}
  
  -- Get source model columns
  {% set source_columns_query %}
    SELECT 
      column_name,
      type as data_type,
      CASE WHEN is_nullable = 'YES' THEN TRUE ELSE FALSE END as is_nullable
    FROM pg_table_def
    WHERE tablename = '{{ source_model }}'
    AND schemaname = '{{ source_relation.schema }}'
    ORDER BY ordinal_position
  {% endset %}
  
  {% set source_columns_result = run_query(source_columns_query) %}
  
  -- Store source columns in a dictionary
  {% set source_cols = {} %}
  {% if execute and source_columns_result and source_columns_result.rows %}
    {% for col in source_columns_result %}
      {% do source_cols.update({col['column_name']: {
        'data_type': col['data_type'],
        'is_nullable': col['is_nullable']
      }}) %}
    {% endfor %}
  {% endif %}
  
  -- Get source row count
  {% set source_count_query %}
    SELECT COUNT(*) FROM {{ source_relation }}
  {% endset %}
  
  {% set source_count_result = run_query(source_count_query) %}
  {% set source_count = source_count_result.columns[0][0] if source_count_result and source_count_result.rows else 0 %}
  
  -- Find common columns between source and target
  {% set common_columns = [] %}
  {% for col_name in source_cols %}
    {% if col_name in target_cols %}
      {% do common_columns.append(col_name) %}
    {% endif %}
  {% endfor %}
  
  -- Find columns that exist in source but not in target (potentially missing data)
  {% set missing_columns = [] %}
  {% for col_name in source_cols %}
    {% if col_name not in target_cols %}
      {% do missing_columns.append(col_name) %}
    {% endif %}
  {% endfor %}
  
  -- Store source model info
  {% do source_models_info.append({
    'name': source_model,
    'columns': source_cols,
    'common_columns': common_columns,
    'missing_columns': missing_columns,
    'row_count': source_count
  }) %}
  
  {% if execute %}
    {{ log('Source model: ' ~ source_model, info=True) }}
    {{ log('- Row count: ' ~ source_count, info=True) }}
    {{ log('- Columns: ' ~ source_cols|length, info=True) }}
    {{ log('- Common columns with target: ' ~ common_columns|length, info=True) }}
    
    {% if missing_columns %}
      {{ log('- WARNING: Columns missing from target model:', info=True) }}
      {% for col in missing_columns %}
        {{ log('  - ' ~ col, info=True) }}
      {% endfor %}
    {% endif %}
    
    {{ log('', info=True) }}
  {% endif %}
{% endfor %}

-- Calculate total row count from all source models
{% set total_source_rows = 0 %}
{% for model_info in source_models_info %}
  {% set total_source_rows = total_source_rows + model_info.row_count %}
{% endfor %}

{% if execute %}
  {{ log('Target model: ' ~ target_model, info=True) }}
  {{ log('- Row count: ' ~ target_count, info=True) }}
  {{ log('- Columns: ' ~ target_cols|length, info=True) }}
  {{ log('- Total source rows: ' ~ total_source_rows, info=True) }}
  {{ log('- Row count difference: ' ~ (target_count - total_source_rows), info=True) }}
  {{ log('', info=True) }}
{% endif %}

-- Determine key columns if not provided
{% if key_columns is none %}
  {% set possible_keys = ['id', 'key', 'pk', 'primary_key'] %}
  {% set source_model_common_columns = [] %}
  
  -- Get common columns from all source models
  {% if source_models_info|length > 0 %}
    {% set source_model_common_columns = source_models_info[0].common_columns %}
    
    {% for model_info in source_models_info %}
      {% if loop.index > 1 %}
        {% set temp_common = [] %}
        {% for col in source_model_common_columns %}
          {% if col in model_info.common_columns %}
            {% do temp_common.append(col) %}
          {% endif %}
        {% endfor %}
        {% set source_model_common_columns = temp_common %}
      {% endif %}
    {% endfor %}
  {% endif %}
  
  -- Attempt to find key columns
  {% set found_keys = [] %}
  {% for col in source_model_common_columns %}
    {% for key_pattern in possible_keys %}
      {% if col|lower is containing(key_pattern) %}
        {% do found_keys.append(col) %}
      {% endif %}
    {% endfor %}
  {% endfor %}
  
  {% if found_keys|length > 0 %}
    {% set key_columns = found_keys %}
  {% elif source_model_common_columns|length > 0 %}
    {% set key_columns = [source_model_common_columns[0]] %}
  {% else %}
    {% set key_columns = [] %}
  {% endif %}
{% endif %}

{% if execute %}
  {{ log('Using key columns for comparison: ' ~ key_columns|join(', '), info=True) }}
  {{ log('', info=True) }}
{% endif %}

-- Perform detailed comparison based on comparison type
{% if comparison_type == 'full' or comparison_type == 'rows' %}
  -- For each source model, check if all its rows are represented in the target model
  {% for model_info in source_models_info %}
    {% set source_model = model_info.name %}
    {% set source_relation = ref(source_model) %}
    
    -- Create a query that finds rows in source not in target
    {% if key_columns|length > 0 %}
      {% set missing_rows_query %}
        WITH source_data AS (
          SELECT 
            {% for key_col in key_columns %}
            {{ key_col }}{% if not loop.last %},{% endif %}
            {% endfor %}
          FROM {{ source_relation }}
        ),
        
        target_data AS (
          SELECT 
            {% for key_col in key_columns %}
            {{ key_col }}{% if not loop.last %},{% endif %}
            {% endfor %}
          FROM {{ target_relation }}
        ),
        
        missing_rows AS (
          SELECT s.*
          FROM source_data s
          LEFT JOIN target_data t ON 
            {% for key_col in key_columns %}
            s.{{ key_col }} = t.{{ key_col }}{% if not loop.last %} AND {% endif %}
            {% endfor %}
          WHERE 
            {% for key_col in key_columns %}
            t.{{ key_col }} IS NULL{% if not loop.last %} OR {% endif %}
            {% endfor %}
        )
        
        SELECT COUNT(*) as missing_row_count FROM missing_rows
      {% endset %}
      
      {% set missing_rows_result = run_query(missing_rows_query) %}
      {% set missing_rows = missing_rows_result.columns[0][0] if missing_rows_result and missing_rows_result.rows else 0 %}
      
      {% if execute %}
        {% if missing_rows > 0 %}
          {{ log('WARNING: Found ' ~ missing_rows ~ ' rows in ' ~ source_model ~ ' that are not in ' ~ target_model ~ ' based on key columns', info=True) }}
          
          -- Get sample of missing rows for debugging
          {% set sample_query %}
            WITH source_data AS (
              SELECT *
              FROM {{ source_relation }}
            ),
            
            target_data AS (
              SELECT 
                {% for key_col in key_columns %}
                {{ key_col }}{% if not loop.last %},{% endif %}
                {% endfor %}
              FROM {{ target_relation }}
            ),
            
            missing_rows AS (
              SELECT s.*
              FROM source_data s
              LEFT JOIN target_data t ON 
                {% for key_col in key_columns %}
                s.{{ key_col }} = t.{{ key_col }}{% if not loop.last %} AND {% endif %}
                {% endfor %}
              WHERE 
                {% for key_col in key_columns %}
                t.{{ key_col }} IS NULL{% if not loop.last %} OR {% endif %}
                {% endfor %}
            )
            
            SELECT * FROM missing_rows
            LIMIT 5
          {% endset %}
          
          {% set sample_rows_result = run_query(sample_query) %}
          
          {{ log('Sample of missing rows:', info=True) }}
          {% if sample_rows_result and sample_rows_result.rows %}
            {% for i in range(sample_rows_result.rows|length) %}
              {% set row_values = [] %}
              {% for col_name in sample_rows_result.column_names %}
                {% do row_values.append(col_name ~ ': ' ~ sample_rows_result.rows[i][loop.index0]) %}
              {% endfor %}
              {{ log('  - {' ~ row_values|join(', ') ~ '}', info=True) }}
            {% endfor %}
          {% endif %}
        {% else %}
          {{ log('✓ All rows from ' ~ source_model ~ ' are represented in ' ~ target_model ~ ' based on key columns', info=True) }}
        {% endif %}
        
        {{ log('', info=True) }}
      {% endif %}
    {% else %}
      {{ log('WARNING: Unable to compare rows between ' ~ source_model ~ ' and ' ~ target_model ~ ' without key columns', info=True) }}
      {{ log('', info=True) }}
    {% endif %}
  {% endfor %}
{% endif %}

{% if comparison_type == 'full' or comparison_type == 'union' %}
  -- Check if the target model contains at least the union of all values from source models
  {% if key_columns|length > 0 %}
    {% set union_query %}
      WITH all_source_keys AS (
        {% for model_info in source_models_info %}
          SELECT 
            {% for key_col in key_columns %}
            {{ key_col }}{% if not loop.last %},{% endif %}
            {% endfor %}
          FROM {{ ref(model_info.name) }}
          
          {% if not loop.last %}UNION{% endif %}
        {% endfor %}
      ),
      
      target_keys AS (
        SELECT 
          {% for key_col in key_columns %}
          {{ key_col }}{% if not loop.last %},{% endif %}
          {% endfor %}
        FROM {{ target_relation }}
      ),
      
      missing_keys AS (
        SELECT s.*
        FROM all_source_keys s
        LEFT JOIN target_keys t ON 
          {% for key_col in key_columns %}
          s.{{ key_col }} = t.{{ key_col }}{% if not loop.last %} AND {% endif %}
          {% endfor %}
        WHERE 
          {% for key_col in key_columns %}
          t.{{ key_col }} IS NULL{% if not loop.last %} OR {% endif %}
          {% endfor %}
      )
      
      SELECT COUNT(*) as missing_union_count FROM missing_keys
    {% endset %}
    
    {% set missing_union_result = run_query(union_query) %}
    {% set missing_union = missing_union_result.columns[0][0] if missing_union_result and missing_union_result.rows else 0 %}
    
    {% if execute %}
      {% if missing_union > 0 %}
        {{ log('WARNING: Found ' ~ missing_union ~ ' unique key combinations from all source models that are missing in the target model', info=True) }}
        
        -- Get sample of missing union rows
        {% set sample_union_query %}
          WITH all_source_keys AS (
            {% for model_info in source_models_info %}
              SELECT 
                {% for key_col in key_columns %}
                {{ key_col }}{% if not loop.last %},{% endif %}
                {% endfor %}
              FROM {{ ref(model_info.name) }}
              
              {% if not loop.last %}UNION{% endif %}
            {% endfor %}
          ),
          
          target_keys AS (
            SELECT 
              {% for key_col in key_columns %}
              {{ key_col }}{% if not loop.last %},{% endif %}
              {% endfor %}
            FROM {{ target_relation }}
          ),
          
          missing_keys AS (
            SELECT s.*
            FROM all_source_keys s
            LEFT JOIN target_keys t ON 
              {% for key_col in key_columns %}
              s.{{ key_col }} = t.{{ key_col }}{% if not loop.last %} AND {% endif %}
              {% endfor %}
            WHERE 
              {% for key_col in key_columns %}
              t.{{ key_col }} IS NULL{% if not loop.last %} OR {% endif %}
              {% endfor %}
          )
          
          SELECT * FROM missing_keys
          LIMIT 5
        {% endset %}
        
        {% set sample_union_result = run_query(sample_union_query) %}
        
        {{ log('Sample of missing union keys:', info=True) }}
        {% if sample_union_result and sample_union_result.rows %}
          {% for i in range(sample_union_result.rows|length) %}
            {% set row_values = [] %}
            {% for col_name in sample_union_result.column_names %}
              {% do row_values.append(col_name ~ ': ' ~ sample_union_result.rows[i][loop.index0]) %}
            {% endfor %}
            {{ log('  - {' ~ row_values|join(', ') ~ '}', info=True) }}
          {% endfor %}
        {% endif %}
      {% else %}
        {{ log('✓ The target model contains all unique key combinations from all source models', info=True) }}
      {% endif %}
      
      {{ log('', info=True) }}
    {% endif %}
  {% endif %}
{% endif %}

{% if comparison_type == 'full' or comparison_type == 'schema' %}
  -- Check for any critical columns that might be missing in the target
  {% set all_source_columns = {} %}
  
  -- Gather all unique columns from all source models
  {% for model_info in source_models_info %}
    {% for col_name, col_info in model_info.columns.items() %}
      {% if col_name not in all_source_columns %}
        {% do all_source_columns.update({col_name: col_info}) %}
      {% endif %}
    {% endfor %}
  {% endfor %}
  
  -- Check for columns missing in target
  {% set critical_missing_columns = [] %}
  {% for col_name, col_info in all_source_columns.items() %}
    {% if col_name not in target_cols and col_name not in key_columns %}
      {% do critical_missing_columns.append(col_name) %}
    {% endif %}
  {% endfor %}
  
  {% if execute %}
    {% if critical_missing_columns %}
      {{ log('WARNING: The following columns from source models are missing in the target model:', info=True) }}
      {% for col in critical_missing_columns %}
        {{ log('  - ' ~ col, info=True) }}
      {% endfor %}
    {% else %}
      {{ log('✓ All critical source columns are represented in the target model', info=True) }}
    {% endif %}
    
    {{ log('', info=True) }}
  {% endif %}
{% endif %}

-- Return a summary table with results
WITH source_summary AS (
  {% for model_info in source_models_info %}
    SELECT
      '{{ model_info.name }}' as model_name,
      '{{ source_models | join(",") }}' as source_models,
      '{{ target_model }}' as target_model,
      {{ model_info.row_count }} as row_count,
      {{ model_info.columns | length }} as column_count,
      {{ model_info.common_columns | length }} as common_columns,
      {{ model_info.missing_columns | length }} as missing_columns
    {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
),

target_summary AS (
  SELECT
    '{{ target_model }}' as model_name,
    '{{ source_models | join(",") }}' as source_models,
    '{{ target_model }}' as target_model,
    {{ target_count }} as row_count,
    {{ target_cols | length }} as column_count,
    NULL as common_columns,
    NULL as missing_columns
),

combined_summary AS (
  SELECT * FROM source_summary
  UNION ALL
  SELECT * FROM target_summary
)

SELECT * FROM combined_summary

{% endmacro %}
