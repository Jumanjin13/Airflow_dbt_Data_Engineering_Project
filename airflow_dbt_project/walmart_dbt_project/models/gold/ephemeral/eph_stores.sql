select
    store_id,
    store_name,
    store_city,
    store_province,
    store_country,
    store_created_timestamp,
    store_updated_timestamp,
    store_is_active,
    store_processed_at,
    current_timestamp() as store_gold_processed_at
from {{ ref('obt_b') }}
where store_id is not null
qualify row_number() over (
    partition by store_id
    order by
        store_updated_timestamp desc nulls last,
        store_processed_at desc nulls last
) = 1
