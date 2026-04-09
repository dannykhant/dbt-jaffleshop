with orders as (
    select * from {{ ref('stg_jaffle_shop_v2__orders') }}
),

order_items as (
    select * from {{ ref('stg_jaffle_shop_v2__order_items') }}
),

final as (
    select
        o.order_id,
        oi.order_item_id,
        o.customer_id,
        o.store_id,
        oi.product_id,
        o.order_date,
        o.order_subtotal,
        o.tax_paid,
        o.order_total
    from orders as o
    left join order_items as oi
    using (order_id)
)

select * from final
