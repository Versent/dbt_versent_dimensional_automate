{%- set yaml_metadata -%}
name: customer
source: __int_dim_type_II__template
{%- endset -%}
{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ versent_automate_dbt_dimensional.dim(
    name = metadata_dict['name'],
    source = metadata_dict['source']
    ) 
}}