with products as (
    select * from {{ ref('stg_jaffle_shop_v2__products') }}
),

supplies as (
    select * from {{ ref('stg_jaffle_shop_v2__supplies') }}
),

final as (

    select
        p.product_id,
        p.product_name,
        p.product_type,
        p.product_desc,
        p.product_price,
        s.supply_name,
        s.supply_cost,
        s.is_perishable
    from products p
    left join supplies s
    using (product_id)

)

select * from final
