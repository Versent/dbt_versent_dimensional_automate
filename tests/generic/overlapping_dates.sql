{% test overlapping_dates(model, column_name , effective_from, effective_to) %}
with
    validation as (
        select
            {{ column_name }},
            {{ effective_to }},
            lead({{ effective_from }})
                over (
                    partition by
                        {{ column_name }}
                    order by
                        {{ effective_from }}
                ) as next_effective_to
                    
        from
            {{ model }}
    )
select
    *
from 
    validation
where
    {{ effective_to }} >= next_effective_to
{% endtest %}

