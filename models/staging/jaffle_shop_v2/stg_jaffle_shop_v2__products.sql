with 

source as (

    select * from {{ source('jaffle_shop_v2', 'products') }}

),

renamed as (

    select
    
        sku as product_id,
        name as product_name,
        type as product_type,
        description as product_desc,
        price as product_price
        
    from source

)

select * from renamed