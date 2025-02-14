{%- set yaml_metadata -%}
name: product
source: __int_dim_product
payload:
    - product_name
    - category
    - unit_price
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