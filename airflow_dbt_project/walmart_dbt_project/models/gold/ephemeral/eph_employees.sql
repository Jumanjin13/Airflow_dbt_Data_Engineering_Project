select
    employee_id,
    store_id as employee_store_id,
    first_name as employee_first_name,
    last_name as employee_last_name,
    email as employee_email,
    job_title as employee_job_title,
    salary as employee_salary,
    created_timestamp as employee_created_timestamp,
    updated_timestamp as employee_updated_timestamp,
    is_active as employee_is_active,
    processed_at as employee_processed_at,
    current_timestamp() as employee_gold_processed_at
from {{ ref('employees_t') }}
where employee_id is not null
qualify row_number() over (
    partition by employee_id
    order by
        updated_timestamp desc nulls last,
        processed_at desc nulls last
) = 1
