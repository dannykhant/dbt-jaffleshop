with 

source as (

    select * from {{ source('jaffle_shop_v2', 'orders') }}

),

renamed as (

    select
        id as order_id,
        customer as customer_id,
        store_id,
        ordered_at as order_date,
        subtotal as order_subtotal,
        tax_paid,
        order_total

    from source

)

select * from renamed