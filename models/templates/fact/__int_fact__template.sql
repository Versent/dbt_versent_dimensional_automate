with sales_order as (
    select 
        1 as bk_sales_order,
        20240101 as bk_order_date,
        20240102 as bk_delivery_date,
        101 as bk_manufacturing_product,
        501 as bk_customer,
        301 as bk_store_key,
        2 as quantity_sold,
        100.00 as total_sales_amount,
        5.00 as discount_amount,
        10.00 as tax_amount,
        95.00 as net_sales_amount,
        cast('2024-01-01 00:00:00'   as timestamp) as load_datetime      
    union all
    select 
        2, 20240102, 20240103, 102, 502, 302, 1, 50.00, 0.00, 5.00, 50.00, cast('2024-01-02 00:00:00'   as timestamp)
    union all
    select 
        3, 20240103, 20240104, 103, 503, 303, 5, 250.00, 25.00, 20.00, 225.00, cast('2024-01-03 00:00:00'   as timestamp)
    union all
    select 
        4, 20240104, 20240105, 104, 504, 304, 3, 150.00, 10.00, 12.00, 140.00, cast('2024-01-04 00:00:00'   as timestamp)
    union all
    select 
        5, 20240105, 20240106, 105, 505, 305, 4, 200.00, 15.00, 18.00, 185.00, cast('2024-01-05 00:00:00'   as timestamp)
)
select
    *,
    '__int_fact__template' as record_source
from
    sales_order
