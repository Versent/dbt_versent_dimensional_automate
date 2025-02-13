with dim_product as (
    select 101 as bk_product, 
            'laptop model x' as product_name, 
            'electronics' as category, 
            1500.00 as unit_price,
            timestamp '1900-01-01 00:00:00' as effective_from, 
            timestamp '2024-05-31 23:59:59' as effective_to, 
            false as is_current
    union all 
        select 101, 'laptop model x pro', 'electronics', 1600.00, timestamp '2024-06-01 00:00:00', timestamp '2999-01-01 00:00:00', true
    union all 
        select 102, 'phone model y', 'electronics', 800.00, timestamp '1900-01-01 00:00:00', timestamp '2999-01-01 00:00:00', true
    union all 
        select 103, 'office chair', 'furniture', 200.00, timestamp '1900-01-01 00:00:00', timestamp '2999-01-01 00:00:00', true
    union all 
        select 104, 'wireless keyboard', 'accessories', 50.00, timestamp '1900-01-01 00:00:00', timestamp '2999-01-01 00:00:00', true
    union all 
        select 105, 'monitor 24-inch', 'electronics', 300.00, timestamp '1900-01-01 00:00:00', timestamp '2999-01-01 00:00:00', true
)
select
    concat('SID_', bk_product) as dim_product_sid,
    * 
from 
    dim_product
