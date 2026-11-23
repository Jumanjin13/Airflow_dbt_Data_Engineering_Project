select
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_email,
    customer_phone,
    customer_city,
    customer_province,
    customer_country,
    customer_created_timestamp,
    customer_updated_timestamp,
    customer_is_active,
    customer_processed_at,
    current_timestamp() as customer_gold_processed_at
from {{ ref('obt_b') }}
where customer_id is not null
qualify row_number() over (
    partition by customer_id
    order by
        customer_updated_timestamp desc nulls last,
        customer_processed_at desc nulls last
) = 1