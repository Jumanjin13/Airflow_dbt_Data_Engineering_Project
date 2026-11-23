select
    product_id,
    product_name,
    product_category,
    product_brand,
    product_price,
    product_created_timestamp,
    product_updated_timestamp,
    product_is_active,
    product_processed_at,
    current_timestamp() as product_gold_processed_at
from {{ ref('obt_b') }}
where product_id is not null
qualify row_number() over (
    partition by product_id
    order by
        product_updated_timestamp desc nulls last,
        product_processed_at desc nulls last
) = 1
