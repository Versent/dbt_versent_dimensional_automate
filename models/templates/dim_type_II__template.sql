{{ config(materialized='incremental')    }}

{%- set yaml_metadata -%}
hash_key: hk_customer
source: __int_dim_customers
payload:
    - customer_id
    - first_name
    - last_name
    - record_source
    - first_order_date
    - most_recent_order_date
    - number_of_orders
    - lifetime_value
record_action: 'DELETE'
{%- endset -%}
{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ versent_automate_dbt_dimensional.dim(
    hash_key= metadata_dict['hash_key'], 
    source = metadata_dict['source'], 
    payload= metadata_dict['payload']
    ) }}