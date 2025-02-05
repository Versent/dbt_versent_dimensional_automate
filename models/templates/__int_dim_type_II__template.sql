with
    seed as (select 1 as business_key),
    customer_1 as (
        select
            business_key as customer_id,
            'Tim' as first_name,
            'Napier' as last_name,
            'seed' record_source,
            '1/1/2005' as first_order_date,
            '1/1/2005' as most_recent_order_date,
            2 as number_of_orders,
            100 as lifetime_value,
            current_timestamp as as_of_date,
            current_timestamp as load_datetime
        from seed
    ),
    union_customers as (select * from customer_1)
select *
from
    union_customers
