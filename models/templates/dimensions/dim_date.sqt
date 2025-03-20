{%- set yaml_metadata -%}
name: date
source: __int_dim_date
payload:
    - calendar_date
    - fiscal_year
    - fiscal_quarter
    - month_name
{%- endset -%}
{% set metadata_dict = fromyaml(yaml_metadata) %}
{{ versent_automate_dbt_dimensional.dim(
    name = metadata_dict['name'],
    source = metadata_dict['source'],
    payload = metadata_dict['payload']
    ) 
}}