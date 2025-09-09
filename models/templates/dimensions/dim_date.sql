-- depends_on: {{ ref('__int_dim_date') }}
{{
    config(
        enabled= false
    )
}}

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
{{ dbt_versent_dimensional_automate.dim(
    name = metadata_dict['name'],
    source = metadata_dict['source'],
    payload = metadata_dict['payload']
    ) 
}}