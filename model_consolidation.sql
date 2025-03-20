-- MODEL CONSOLIDATION VERIFICATION MACRO
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

-- Get target model columns
{% set target_columns_query %}
  SELECT 
    column_name,
    data_type,
    is_nullable
  FROM {{ information_schema_columns(target_relation) }}
  ORDER BY ordinal_position
{% endset %}

{% set target_columns = run_query(target_columns_query) %}

-- Store target model columns in a dictionary for easy lookup
{% set target_cols = {} %}
{% for col in target_columns %}
  {% do target_cols.update({col['column_name']: {
    'data_type': col['data_type'],
    'is_nullable': col['is_nullable']
  }}) %}
{% endfor %}

-- Get target row count
{% set target_count_query %}
  SELECT COUNT(*) FROM {{ target_relation }}
{% endset %}

{% set target_count = run_query(target_count_query).columns[0][0] %}

-- Store source model info
{% set source_models_info = [] %}

-- Process each source model
{% for source_model in source_models %}
  {% set source_relation = ref(source_model) %}
  
  -- Get source model columns
  {% set source_columns_query %}
    SELECT 
      column_name,
      data_type,
      is_nullable
    FROM {{ information_schema_columns(source_relation) }}
    ORDER BY ordinal_position
  {% endset %}
  
  {% set source_columns = run_query(source_columns_query) %}
  
  -- Store source columns in a dictionary
  {% set source_cols = {} %}
  {% for col in source_columns %}
    {% do source_cols.update({col['column_name']: {
      'data_type': col['data_type'],
      'is_nullable': col['is_nullable']
    }}) %}
  {% endfor %}
  
  -- Get source row count
  {% set source_count_query %}
    SELECT COUNT(*) FROM {{ source_relation }}
  {% endset %}
  
  {% set source_count = run_query(source_count_query).columns[0][0] %}
  
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
  {% else %}
    {% set key_columns = source_model_common_columns %}
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
          SELECT 
            {% for key_col in key_columns %}
            {{ key_col }}{% if not loop.last %},{% endif %}
            {% endfor %}
          FROM source_data
          
          EXCEPT
          
          SELECT 
            {% for key_col in key_columns %}
            {{ key_col }}{% if not loop.last %},{% endif %}
            {% endfor %}
          FROM target_data
        )
        
        SELECT COUNT(*) as missing_row_count FROM missing_rows
      {% endset %}
      
      {% set missing_rows = run_query(missing_rows_query).columns[0][0] %}
      
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
              WHERE NOT EXISTS (
                SELECT 1
                FROM target_data t
                WHERE 
                  {% for key_col in key_columns %}
                  s.{{ key_col }} = t.{{ key_col }}{% if not loop.last %} AND {% endif %}
                  {% endfor %}
              )
            )
            
            SELECT * FROM missing_rows
            LIMIT 5
          {% endset %}
          
          {% set sample_rows = run_query(sample_query) %}
          
          {{ log('Sample of missing rows:', info=True) }}
          {% for row in sample_rows %}
            {{ log('  ' ~ row, info=True) }}
          {% endfor %}
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
        SELECT 
          {% for key_col in key_columns %}
          {{ key_col }}{% if not loop.last %},{% endif %}
          {% endfor %}
        FROM all_source_keys
        
        EXCEPT
        
        SELECT 
          {% for key_col in key_columns %}
          {{ key_col }}{% if not loop.last %},{% endif %}
          {% endfor %}
        FROM target_keys
      )
      
      SELECT COUNT(*) as missing_union_count FROM missing_keys
    {% endset %}
    
    {% set missing_union = run_query(union_query).columns[0][0] %}
    
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
            SELECT 
              {% for key_col in key_columns %}
              {{ key_col }}{% if not loop.last %},{% endif %}
              {% endfor %}
            FROM all_source_keys
            
            EXCEPT
            
            SELECT 
              {% for key_col in key_columns %}
              {{ key_col }}{% if not loop.last %},{% endif %}
              {% endfor %}
            FROM target_keys
          )
          
          SELECT * FROM missing_keys
          LIMIT 5
        {% endset %}
        
        {% set sample_union = run_query(sample_union_query) %}
        
        {{ log('Sample of missing union keys:', info=True) }}
        {% for row in sample_union %}
          {{ log('  ' ~ row, info=True) }}
        {% endfor %}
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
      '{{ source_models_info | join(",") }}' as source_models,
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
    '{{ source_models_info | join(",") }}' as source_models,
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
