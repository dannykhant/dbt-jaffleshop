with orders as (
    select * from {{ ref('stg_jaffle_shop__orders') }}
),

payments as (
    select * from {{ ref('stg_stripe__payments') }}
),

order_payments as (
    select 
        order_id,
        sum(case when payment_status = 'success' then payment_amount end) as payment_amount
    from payments
    group by 1
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        p.payment_amount
    from orders as o
    left join order_payments as p
    using (order_id)
)

select * from final