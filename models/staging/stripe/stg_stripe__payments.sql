with 

source as (

    select * from {{ source('stripe', 'payments') }}

),

renamed as (

    select
        id as payment_id,
        orderid as order_id,
        paymentmethod as payment_method,
        status as payment_status,
        -- cents to dollars
        amount / 100 as payment_amount,
        created as payment_date

    from source

)

select * from renamed