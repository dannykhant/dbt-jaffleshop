with

    -- Import CTEs
    customers as (select * from {{ source("jaffle_shop", "customers") }}),

    orders as (select * from {{ source("jaffle_shop", "orders") }}),

    payments as (select * from {{ source("stripe", "payments") }}),

    -- Logical CTEs
    success_payments as (
        select
            orderid as order_id,
            max(created) as payment_finalized_date,
            sum(amount) / 100.0 as total_amount_paid
        from payments
        where status <> 'fail'
        group by 1
    ),

    paid_orders as (
        select
            orders.id as order_id,
            orders.user_id as customer_id,
            orders.order_date as order_placed_at,
            orders.status as order_status,
            payments.total_amount_paid,
            payments.payment_finalized_date,
            customers.first_name as customer_first_name,
            customers.last_name as customer_last_name
        from orders
        left join success_payments as payments on orders.id = payments.order_id
        left join customers on orders.user_id = customers.id
    ),

    customer_lifetime_value as (
        select
            order_id,
            customer_id,
            sum(total_amount_paid) over (
                partition by customer_id order by order_id
            ) as clv_good
        from paid_orders
        order by order_id
    ),

    -- Final CTE
    final as (
        select
            -- identifier
            paid_orders.order_id,
            paid_orders.customer_id,

            row_number() over (order by paid_orders.order_id) as transaction_seq,
            row_number() over (
                partition by paid_orders.customer_id order by paid_orders.order_id
            ) as customer_sales_seq,

            -- timestamp
            paid_orders.order_placed_at,

            -- orders
            paid_orders.order_status,

            -- payments
            paid_orders.total_amount_paid,
            paid_orders.payment_finalized_date,

            -- customers
            paid_orders.customer_first_name,
            paid_orders.customer_last_name,

            -- new vs. returning customer
            case
                when
                    rank() over (
                        partition by paid_orders.customer_id
                        order by paid_orders.order_placed_at, paid_orders.order_id
                    )
                    = 1
                then 'new'
                else 'return'
            end as nvsr,

            -- customer life time value
            customer_lifetime_value.clv_good as customer_lifetime_value,

            -- first order date
            min(paid_orders.order_placed_at) over (
                partition by paid_orders.customer_id
            ) as fdos
            
        from paid_orders
        left outer join
            customer_lifetime_value
            on customer_lifetime_value.order_id = paid_orders.order_id
        order by paid_orders.order_id
    )

-- Simple Select Statment
select *
from final
