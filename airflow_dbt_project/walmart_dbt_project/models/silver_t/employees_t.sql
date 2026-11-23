{{
    config(
    materialized='incremental',
    unique_key='employee_id'
    )
}}

select 
    *,
    current_timestamp() as processed_at
from 
    {{source('walmart_databricks', 'employees')}}
{% if is_incremental() %}
  where updated_timestamp > coalesce((select max(updated_timestamp) from {{ this }}), '1900-01-01')
{% endif %}
  
