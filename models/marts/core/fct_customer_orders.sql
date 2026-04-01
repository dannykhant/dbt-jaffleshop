with
    customers as (select * from {{ ref("int_customers") }}),

    paid_orders as (select * from {{ ref("int_orders") }}),

    customer_orders as (

        select
            paid_orders.order_id,
            paid_orders.customer_id,
            paid_orders.order_date,
            paid_orders.order_status,
            paid_orders.total_amount_paid,
            paid_orders.payment_finalized_date,
            customers.first_name,
            customers.last_name,

            -- sales transaction sequence
            row_number() over (
                order by paid_orders.order_date, paid_orders.order_id
            ) as transaction_seq,

            -- customer sales sequence
            row_number() over (
                partition by paid_orders.customer_id
                order by paid_orders.order_date, paid_orders.order_id
            ) as customer_sales_seq,

            -- new vs returning customer
            case
                when
                    (
                        rank() over (
                            partition by paid_orders.customer_id
                            order by paid_orders.order_date, paid_orders.order_id
                        )
                        = 1
                    )
                then 'new'
                else 'return'
            end as nvsr,

            -- customer lifetime value
            sum(paid_orders.total_amount_paid) over (
                partition by paid_orders.customer_id
                order by paid_orders.order_date, paid_orders.order_id
            ) as customer_lifetime_value,

            -- first day of sale
            first_value(paid_orders.order_date) over (
                partition by paid_orders.customer_id
                order by paid_orders.order_date, paid_orders.order_id
            ) as fdos

        from paid_orders
        left join customers on paid_orders.customer_id = customers.customer_id
    ),

    final as (

        select

            customer_id,
            order_id,
            order_date as order_placed_at,
            order_status,
            total_amount_paid,
            payment_finalized_date,
            first_name as customer_first_name,
            last_name as customer_last_name,
            transaction_seq,
            customer_sales_seq,
            nvsr,
            customer_lifetime_value,
            fdos

        from customer_orders

    )

select * from final