with 

source as (

    select * from {{ source('jaffle_shop_v2', 'items') }}

),

renamed as (

    select
        id as order_item_id,
        order_id,
        sku as product_id

    from source

)

select * from renamed