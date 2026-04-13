with 

source as (

    select * from {{ source('jaffle_shop_v2', 'stores') }}

),

renamed as (

    select
        id as store_id,
        name as store_name,
        opened_at as store_opened_at,
        tax_rate

    from source

)

select * from renamed