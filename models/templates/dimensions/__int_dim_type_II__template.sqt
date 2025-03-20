with
    seed as (select 1 as business_key),
    customer_1 as (
        select
            business_key as bk_customer,
            'Tim' as first_name,
            'Napier' as last_name,
            'seed' record_source,
            '1/1/2005' as first_order_date,
            '1/1/2005' as most_recent_order_date,
            2 as number_of_orders,
            100 as lifetime_value,
            to_timestamp('2025-01-01 09:00:00') as as_of_date,
            current_timestamp as load_datetime
        from seed
    ),
    customer_2 as (
        select
            business_key as bk_customer,
            'Tim' as first_name,
            'Napier' as last_name,
            'seed' record_source,
            '1/1/2005' as first_order_date,
            '1/3/2005' as most_recent_order_date,
            2 as number_of_orders,
            100 as lifetime_value,
            to_timestamp('2025-03-01 09:00:00') as as_of_date,
            current_timestamp as load_datetime
        from seed
    ),
    union_customers as (
        select * from customer_1
        union
        select * from customer_2
        )
select *
from
    union_customers
