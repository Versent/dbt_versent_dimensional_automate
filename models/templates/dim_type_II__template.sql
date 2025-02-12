{{ config(materialized='incremental')    }}

{%- set yaml_metadata -%}
dimension_name: customer
business_key: customer_id
source: __int_dim_type_II__template
payload:
    - first_name
    - last_name
    - first_order_date
    - most_recent_order_date
    - number_of_orders
    - lifetime_value
record_action: 'DELETE'
{%- endset -%}
{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ versent_automate_dbt_dimensional.dim_fred(
    dimension_name = metadata_dict['dimension_name'], 
    business_key= metadata_dict['business_key'], 
    source = metadata_dict['source'], 
    payload= metadata_dict['payload']
    ) }}