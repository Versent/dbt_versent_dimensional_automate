with 
    dim_date as (
        select 20240101 as dim_date_sid, 
            '2024-01-01' as calendar_date, 
            2024 as fiscal_year, 
            'q3' as fiscal_quarter, 
            'january' as month_name
        union all 
            select 20240102, '2024-01-02', 2024, 'q3', 'january'
        union all 
            select 20240103, '2024-01-03', 2024, 'q3', 'january'
        union all 
            select 20240104, '2024-01-04', 2024, 'q3', 'january'
        union all 
            select 20240105, '2024-01-05', 2024, 'q3', 'january'
        union all 
            select 20240106, '2024-01-06', 2024, 'q3', 'january'
)
select 
    * 
from 
    dim_date
