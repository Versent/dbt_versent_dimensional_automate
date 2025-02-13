-- depends_on: {{ ref('__int_fact__template') }}
{{ config(
    materialized = 'table',
    unique_key = 'bk_sales_order'
)}}

{%- set yaml_metadata -%}
source:             __int_fact__template
name:               sales_order
primary_key:        bk_sales_order

# dimensions
type1:
    dimensions:
        - dim_date:
            question: when
            join_key: bk_date
            role_playing:
                order_date:     
                delivery_date:
type2:
    dimensions:
        - dim_customer:
            question: who
            join_key: bk_customer
        - dim_product:
            question: what
            join_key: bk_product
            role_playing:
                manufacturing_products:    
    dimension_as_of_date: bk_order_date  
       

payload:
    - quantity_sold
    - total_sales_amount
    - discount_amount
    - tax_amount
    - net_sales_amt:  total_sales_amount - discount_amount      

{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ fact(
    source=metadata_dict['source'],
    name=metadata_dict['name'],
    primary_key=metadata_dict['primary_key'],
    dimensions=metadata_dict['dimensions'],
    payload=metadata_dict['payload'],
) }}