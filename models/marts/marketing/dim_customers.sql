with customers as (
    select * from {{ ref('stg_jaffle_shop__customers') }}
),

orders as (
    select * from {{ ref('stg_jaffle_shop__orders') }}
),

payments as (
    select * from {{ ref('stg_stripe__payments') }}
),

customer_orders as (
    select
        o.customer_id,
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date,
        count(o.order_id) as number_of_orders,
        sum(p.amount) as lifetime_value
    from orders as o
    join payments as p
    group by 1
),

final as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        co.first_order_date,
        co.last_order_date,
        coalesce(co.number_of_orders, 0) as number_of_orders,
        coalesce(co.lifetime_value, 0) as lifetime_value
    from customers as c
    left join customer_orders co
    using (customer_id)
)

select * from final