with orders as (
    select * from {{ ref('stg_jaffle_shop__orders') }}
),

payments as (
    select * from {{ ref('stg_stripe__payments') }}
),

final as (
    select
        o.order_id,
        o.customer_id,
        p.amount
    from orders as o
    join payments as p
    using (order_id)
)

select * from final