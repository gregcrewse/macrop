-- models/sources.yml
version: 2

sources:
  - name: risk_data
    description: "Risk data source from Workday exports"
    schema: "bdc_glue.bdc_gsheets"  # Adjust to your actual schema
    tables:
      - name: risk_to
        description: "Time off request data from Workday exports in Google Sheets"
        loaded_at_field: "timestamp_unix"  # Used for incremental loading logic
        
      - name: risk_loa
        description: "Leave of absence data from Workday exports in Google Sheets"
        loaded_at_field: "timestamp_unix"  # Used for incremental loading logic

  - name: workday
    description: "Employee data from Workday"
    schema: "bdc_glue.workday"
    tables:
      - name: cx_employee_workday_list
        description: "Employee information from Workday"
        
  - name: date_dimension
    description: "Date dimension table"
    schema: "bdc_redshift.bdc_core"
    tables:
      - name: bdc_date_dim
        description: "Date dimension with holidays and weekday flags"

-- models/staging/stg_risk_to.sql
{{
  config(
    materialized = 'incremental',
    unique_key = 'upi',
    incremental_strategy = 'merge'
  )
}}

WITH source_data AS (
  SELECT
    CAST("Employee ID" AS INTEGER) AS employee_id,
    CAST("Timestamp - Request was Made" AS INTEGER) AS timestamp_request_was_made,
    CAST("Type" AS VARCHAR) AS type,
    CAST("Time Off Date" AS DATE) AS time_off_date,
    CAST("Duration" AS DECIMAL(5,2)) AS duration,
    CAST("Unit of Time" AS VARCHAR) AS unit_of_time,
    CAST("Status" AS VARCHAR) AS status,
    CAST("timestamp_unix" AS BIGINT) AS timestamp_unix,
    -- Create a robust unique identifier
    CONCAT(
      CAST("Employee ID" AS VARCHAR), 
      '|', 
      CAST("Time Off Date" AS VARCHAR),
      '|',
      CAST("Duration" AS VARCHAR)
    ) AS upi,
    current_timestamp() AS dbt_loaded_at
  FROM {{ source('risk_data', 'risk_to') }}
  
  {% if is_incremental() %}
  WHERE 
    -- Only process new records since last run based on timestamp_unix
    -- This is what makes it handle incremental loads
    CAST("timestamp_unix" AS BIGINT) > (
      SELECT COALESCE(MAX(timestamp_unix), 0) FROM {{ this }}
    )
  {% endif %}
),

-- This adds business logic for the 60/30 day window
filtered_data AS (
  SELECT *
  FROM source_data
  WHERE time_off_date BETWEEN 
    DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) AND 
    DATE_ADD(CURRENT_DATE(), INTERVAL 30 DAY)
)

SELECT * FROM filtered_data

-- models/staging/stg_risk_loa.sql
{{
  config(
    materialized = 'incremental',
    unique_key = ['employee_id', 'effective_date'],
    incremental_strategy = 'merge'
  )
}}

WITH source_data AS (
  SELECT
    CAST("Employee ID" AS INTEGER) AS employee_id,
    CAST("Effective Date" AS DATE) AS effective_date,
    CAST("Last Day of Work" AS DATE) AS last_day_of_work,
    CAST("First Day of Leave" AS DATE) AS first_day_of_leave,
    CAST("Last Day of Leave - Estimated" AS DATE) AS last_day_leave_estimated,
    CAST("Last Day of Leave - Actual" AS DATE) AS last_day_leave_actual,
    CAST("First day Back at Work" AS DATE) AS first_day_back_at_work,
    CAST("Total Days on Leave" AS VARCHAR) AS total_days_on_leave,
    CAST("timestamp_unix" AS BIGINT) AS timestamp_unix,
    current_timestamp() AS dbt_loaded_at
  FROM {{ source('risk_data', 'risk_loa') }}

  {% if is_incremental() %}
  WHERE 
    -- Only process new records since last run based on timestamp_unix
    CAST("timestamp_unix" AS BIGINT) > (
      SELECT COALESCE(MAX(timestamp_unix), 0) FROM {{ this }}
    )
  {% endif %}
),

-- This adds business logic for the 60/30 day window
filtered_data AS (
  SELECT *
  FROM source_data
  WHERE effective_date BETWEEN 
    DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) AND 
    DATE_ADD(CURRENT_DATE(), INTERVAL 30 DAY)
)

SELECT * FROM filtered_data

-- models/staging/stg_employee_data.sql
{{
  config(
    materialized = 'view'
  )
}}

SELECT
  employee_id,
  preferred_name AS employee_name
FROM {{ source('workday', 'cx_employee_workday_list') }}

-- models/marts/dim_risk_loa.sql (Slowly Changing Dimension for LOA)
{{
  config(
    materialized = 'table',
    unique_key = ['employee_id', 'effective_date']
  )
}}

WITH current_data AS (
  SELECT * FROM {{ ref('stg_risk_loa') }}
),

employee_data AS (
  SELECT
    employee_id,
    employee_name
  FROM {{ ref('stg_employee_data') }}
),

-- Join with employee data for names
enriched_data AS (
  SELECT
    cd.employee_id,
    ed.employee_name,
    cd.effective_date,
    cd.last_day_of_work,
    cd.first_day_of_leave,
    cd.last_day_leave_estimated,
    cd.last_day_leave_actual,
    cd.first_day_back_at_work,
    cd.total_days_on_leave,
    cd.timestamp_unix,
    cd.dbt_loaded_at,
    -- Add a valid_from and valid_to for SCD tracking
    cd.dbt_loaded_at AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current
  FROM current_data cd
  LEFT JOIN employee_data ed ON cd.employee_id = ed.employee_id
)

SELECT * FROM enriched_data

-- models/marts/dim_risk_to.sql (Slowly Changing Dimension for Time Off)
{{
  config(
    materialized = 'table',
    unique_key = 'upi'
  )
}}

WITH current_data AS (
  SELECT * FROM {{ ref('stg_risk_to') }}
),

employee_data AS (
  SELECT
    employee_id,
    employee_name
  FROM {{ ref('stg_employee_data') }}
),

-- Join with employee data for names
enriched_data AS (
  SELECT
    cd.employee_id,
    ed.employee_name,
    cd.timestamp_request_was_made,
    cd.type,
    cd.time_off_date,
    cd.duration,
    cd.unit_of_time,
    cd.status,
    cd.timestamp_unix,
    cd.upi,
    cd.dbt_loaded_at,
    -- Add a valid_from and valid_to for SCD tracking
    cd.dbt_loaded_at AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current
  FROM current_data cd
  LEFT JOIN employee_data ed ON cd.employee_id = ed.employee_id
)

SELECT * FROM enriched_data

-- models/marts/fct_risk_time_off_loa.sql (Combined fact table)
{{
  config(
    materialized = 'table',
    unique_key = 'upi'
  )
}}

WITH time_off_data AS (
  -- Get only current records from time off dimension
  SELECT
    employee_id,
    employee_name,
    CAST(from_unixtime(timestamp_unix) AS TIMESTAMP(0)) AS request_timestamp,
    timestamp_unix AS request_timestamp_unix,
    type AS type_of_time_off,
    time_off_date,
    duration AS duration_hours,
    status,
    upi
  FROM {{ ref('dim_risk_to') }}
  WHERE is_current = TRUE
),

loa_base AS (
  -- Get only current records from LOA dimension
  SELECT
    loa.employee_id,
    loa.employee_name,
    CAST(from_unixtime(loa.timestamp_unix) AS TIMESTAMP(0)) AS request_timestamp,
    loa.timestamp_unix AS request_timestamp_unix,
    'LOA' AS type_of_time_off,
    loa.first_day_of_leave,
    COALESCE(loa.last_day_leave_actual, loa.last_day_leave_estimated) AS last_day_of_leave
  FROM {{ ref('dim_risk_loa') }} loa
  WHERE is_current = TRUE
),

loa_dates AS (
  -- Expand LOA records to individual days using date dimension
  SELECT
    loa.employee_id,
    loa.employee_name,
    loa.request_timestamp,
    loa.request_timestamp_unix,
    loa.type_of_time_off,
    date_dim.full_date AS time_off_date,
    8.0 AS duration_hours, -- Standard 8-hour workday
    'Approved' AS status,
    CONCAT(
      CAST(loa.employee_id AS VARCHAR),
      '|',
      CAST(date_dim.full_date AS VARCHAR),
      '|8.0'
    ) AS upi
  FROM loa_base loa
  INNER JOIN {{ source('date_dimension', 'bdc_date_dim') }} date_dim
    ON date_dim.full_date BETWEEN loa.first_day_of_leave AND loa.last_day_of_leave
    AND date_dim.day_is_weekday = 1
    AND date_dim.holiday_us_federal_reserve = 0
),

combined_data AS (
  -- Time off data
  SELECT
    employee_id,
    employee_name,
    request_timestamp,
    request_timestamp_unix,
    type_of_time_off,
    time_off_date,
    duration_hours,
    status,
    upi
  FROM time_off_data
  
  UNION ALL
  
  -- LOA expanded data
  SELECT
    employee_id,
    employee_name,
    request_timestamp,
    request_timestamp_unix,
    type_of_time_off,
    time_off_date,
    duration_hours,
    status,
    upi
  FROM loa_dates
)

SELECT
  employee_id AS emp_id,
  employee_name AS emp_name,
  request_timestamp,
  request_timestamp_unix,
  type_of_time_off,
  time_off_date,
  duration_hours,
  status,
  upi,
  current_timestamp() AS last_updated_at
FROM combined_data

-- models/schema.yml (data tests)
version: 2

models:
  - name: stg_risk_to
    description: "Standardized time off request data"
    tests:
      - unique:
          column_name: upi
    columns:
      - name: employee_id
        tests:
          - not_null
      - name: time_off_date
        tests:
          - not_null
      - name: duration
        tests:
          - not_null
      - name: upi
        tests:
          - not_null
          - unique
          
  - name: stg_risk_loa
    description: "Standardized leave of absence data"
    tests:
      - unique:
          column_name: [employee_id, effective_date]
    columns:
      - name: employee_id
        tests:
          - not_null
      - name: effective_date
        tests:
          - not_null
      - name: first_day_of_leave
        tests:
          - not_null
          
  - name: dim_risk_to
    description: "Time off dimension (slowly changing)"
    tests:
      - unique:
          column_name: upi
    columns:
      - name: employee_id
        tests:
          - not_null
      - name: upi
        tests:
          - not_null
          - unique
          
  - name: dim_risk_loa
    description: "Leave of absence dimension (slowly changing)"
    tests:
      - unique:
          column_name: [employee_id, effective_date]
    columns:
      - name: employee_id
        tests:
          - not_null
          - relationships:
              to: ref('stg_employee_data')
              field: employee_id
              
  - name: fct_risk_time_off_loa
    description: "Combined fact table for time off and leave"
    tests:
      - unique:
          column_name: upi
    columns:
      - name: emp_id
        tests:
          - not_null
      - name: time_off_date
        tests:
          - not_null
      - name: upi
        tests:
          - not_null
          - unique
