with 

source as (

    select * from {{ source('jaffle_shop_v2', 'customers') }}

),

renamed as (

    select
        id as customer_id,
        name as full_name

    from source

)

select * from renamed