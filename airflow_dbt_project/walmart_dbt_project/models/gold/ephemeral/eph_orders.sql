select
    order_id,
    order_payment_method,
    order_status,
    order_total_amount,
    order_timestamp,
    order_created_timestamp,
    order_updated_timestamp,
    order_is_active,
    order_processed_at,
    obt_b_processed_at,
    current_timestamp() as order_gold_processed_at
from {{ ref('obt_b') }}
where order_id is not null
qualify row_number() over (
    partition by order_id
    order by
        order_updated_timestamp desc nulls last,
        order_processed_at desc nulls last
) = 1
