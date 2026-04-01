with
    raw_customers as (select * from {{ ref("stg_jaffle_shop__customers") }}),

    paid_orders as (select * from {{ ref("int_orders") }}),

    customers as (

        select
            paid_orders.customer_id,
            raw_customers.first_name,
            raw_customers.last_name,

            -- order count
            count(*) as order_count,

            -- non returned order count
            sum(
                nvl2(paid_orders.valid_order_date, 1, 0)
            ) as non_returned_order_count,

            -- non returned order value
            sum(
                nvl2(paid_orders.valid_order_date, paid_orders.total_amount_paid, 0)
            ) as non_returned_order_value

        from raw_customers
        left join paid_orders on raw_customers.customer_id = paid_orders.customer_id
        group by 
            paid_orders.customer_id,
            raw_customers.first_name,
            raw_customers.last_name
    ),

    avg_order_values as (

        select

            *,
            {{ function('safe_divide') }}(
                non_returned_order_value, 
                non_returned_order_count
            ) as avg_non_returned_order_value

        from customers

    )

select * from avg_order_values