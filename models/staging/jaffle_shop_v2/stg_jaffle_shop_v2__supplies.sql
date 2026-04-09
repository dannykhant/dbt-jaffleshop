with 

source as (

    select * from {{ source('jaffle_shop_v2', 'supplies') }}

),

renamed as (

    select
        id as supply_id,
        name as supply_name,
        sku as product_id,
        cost as supply_cost,
        perishable as is_perishable

    from source

)

select * from renamed