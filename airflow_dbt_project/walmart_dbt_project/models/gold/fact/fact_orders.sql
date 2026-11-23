SELECT 
    order_id,
    order_item_id,
    product_id,
    store_id,
    customer_id,
    order_total_amount,
    order_item_quantity,
    order_item_unit_price,
    order_item_line_amount
FROM 
    {{ ref('obt_b') }}