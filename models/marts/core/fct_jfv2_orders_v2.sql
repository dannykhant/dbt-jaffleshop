with orders as (
    select * from {{ ref('stg_jaffle_shop_v2__orders') }}
),

stores as (
    select * from {{ ref('stg_jaffle_shop_v2__stores') }}
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.store_id,

        cast(o.order_date as date) as order_date,

        s.store_name,
        s.tax_rate,

        o.order_subtotal,
        o.tax_paid,
        o.order_total as order_amount

    from orders as o
    left join stores as s
    using (store_id)
)

select * from final
