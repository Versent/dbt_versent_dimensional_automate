{%- set yaml_metadata -%}
name: customer
source: __int_dim_type_II__template
payload:
    - first_name
    - last_name
    - first_order_date
    - most_recent_order_date
    - number_of_orders
    - lifetime_value
type2:
    as_of_date: as_of_date
{%- endset -%}
{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ versent_automate_dbt_dimensional.dim(
    name    = metadata_dict['name'],
    source  = metadata_dict['source'],
    payload = metadata_dict['payload'],
    type2   = metadata_dict['type2']
    ) 
}}