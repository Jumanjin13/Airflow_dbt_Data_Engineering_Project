{{ config(severity = 'warn') }}

select 
    order_id,
    customer_id,
    order_item_id,
    product_id,
    store_id
from {{ ref('obt_b') }}
where order_id is null
   or customer_id is null
   or order_item_id is null
   or product_id is null
   or store_id is null