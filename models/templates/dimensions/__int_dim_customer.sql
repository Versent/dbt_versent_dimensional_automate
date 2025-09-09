{{
    config(
        enabled = false
    )
}}

with dim_customer as (
    select 501 as bk_customer, 
            'abc pty ltd' as customer_name, 
            'enterprise' as customer_type, 
            'australia' as country,
            to_timestamp('2024-01-01 09:00:00') as as_of_date
    union all 
        select 501, 'abc global ltd', 'enterprise', 'australia', timestamp '2025-01-01 00:00:00'
    union all 
        select 502, 'xyz corp', 'smb', 'new zealand', timestamp '2024-01-01 09:00:00'
    union all 
        select 503, 'def industries', 'enterprise', 'australia', timestamp '2024-01-01 09:00:00'
    union all 
        select 504, 'lmn solutions', 'smb', 'australia', timestamp '2024-01-01 09:00:00'
    union all 
        select 505, 'pqr global', 'enterprise', 'new zealand', timestamp '2024-01-01 09:00:00'
)
select
    *,
    '__int_dim_customer' as record_source,
    current_timestamp() as load_datetime    
from 
    dim_customer
