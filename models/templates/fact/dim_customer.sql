with dim_customer as (
    select 501 as bk_customer, 
            'abc pty ltd' as customer_name, 
            'enterprise' as customer_type, 
            'australia' as country,
            timestamp '1900-01-01 00:00:00' as effective_from_timestamp, 
            timestamp '2024-06-30 23:59:59' as effective_to_timestamp, 
            false as is_current
    union all 
        select 501, 'abc global ltd', 'enterprise', 'australia', timestamp '2024-07-01 00:00:00', timestamp '2999-01-01 00:00:00', true
    union all 
        select 502, 'xyz corp', 'smb', 'new zealand', timestamp '1900-01-01 00:00:00', timestamp '2999-01-01 00:00:00', true
    union all 
        select 503, 'def industries', 'enterprise', 'australia', timestamp '1900-01-01 00:00:00', timestamp '2999-01-01 00:00:00', true
    union all 
        select 504, 'lmn solutions', 'smb', 'australia', timestamp '1900-01-01 00:00:00', timestamp '2999-01-01 00:00:00', true
    union all 
        select 505, 'pqr global', 'enterprise', 'new zealand', timestamp '1900-01-01 00:00:00', timestamp '2999-01-01 00:00:00', true
)
select
    md5(concat(bk_customer , '|', effective_from_timestamp)) as dim_customer_sid,
    *
from 
    dim_customer
