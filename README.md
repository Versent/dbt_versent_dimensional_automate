# Dimensional Model Automation Macros by Versent

Streamline Your Dimensional Modeling with Automation! 
This package provides dbt macros that automate the creation of dimensional models, 
including Type 1 and Type 2 Slowly Changing Dimensions (SCDs) and fact tables. By leveraging these macros, you can streamline 
your data modeling process while maintaining data integrity, historical accuracy, and consistency across your dbt projects.

What does this package offer?
 -  Increased Productivity - Automates the creation of Type 1 & Type 2 dimensions and fact tables, reducing manual effort.
 -  Data Integrity & Historical Tracking - Ensures accurate Type 2 tracking with effective and expiration dates.
 -  Consistency Across Models - Standardized implementation of dimensions and facts simplifies data pipelines.
 -  Seamless Integration with dbt - Uses dbt's built-in features for version control, documentation, and testing.
 -  Scalability - Designed to handle large datasets efficiently while supporting multi-threaded execution.

### Table of Contents
- dim Macro (Type 1 or Type 2)
- fact

## dim Macro (Type 1 or Type 2)

This macro creates either a Type 1 or Type 2 dimension table:

- Type 1 dimensions always contain the latest record for each business key.
- Type 2 dimensions track changes in attributes over time with effective and expiration dates.

Key Benefits:

- Eliminates the need for manually coding SCD logic.
- Supports both business key-based and role-playing dimensions.
- Adds audit columns (record_source, load_datetime, etc.) for improved data governance.

Parameters:
 - name (string): The name of the dimension table.
 - source (string): The name of the source model.
 - payload (list): The list of columns to include in the dimension.
 - type2 (list): The list of columns for Type 2 tracking. Requires at least the as_of_date column name.

Output:
- Type 1: A dimension table containing the latest attribute values, with audit columns such as record_source and load_datetime.
- Type 2: A dimension table with effective_from_datetime, effective_to_datetime, and an is_current flag.


Example Macro Invocation:

```bash

# Provide metadata
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

# Call the macro
{{ versent_automate_dbt_dimensional.dim(
    name    = metadata_dict['name'],
    source  = metadata_dict['source'],
    payload = metadata_dict['payload'],
    type2   = metadata_dict['type2']
    ) 
}}

```


## fact Macro

This macro automates fact table creation by linking Type 1 and Type 2 dimensions. 
It generates a fact table with Type 2 dimension support, ensuring that each fact row correctly aligns with dimension changes.

Key Benefits:

- Simplifies fact table design by automatically integrating Type 1 & Type 2 dimensions.
- Ensures historical accuracy by aligning facts with the correct dimensional state.
- Supports role-playing dimensions (e.g., Order Date vs. Delivery Date).

Parameters:
 - source (string): The source model.
 - name (string): The name of the fact table.
 - primary_key (string): The name of the primary key column.
 - type1_dimensions (list): A list of Type 1 dimensions.
    - question (string) - Represents the business question that this dimension answers.
    - join_key (string) - Primary Key used to join the fact table to the dimension.
    - role_playing (dictionary, optional) = Specifies role-playing columns for dimension that server multiple contextual roles in
                                            in the fact table
 - type2_dimensions (list): A list of Type 2 dimensions.
 - payload (list): A list of measure columns.

Output:
A fact table with proper dimension keys and measures.

Example Macro Invocation:

```bash

# Provide metadata
{%- set yaml_metadata -%}
source:             __int_fact__template
name:               sales_order
primary_key:        bk_sales_order

# dimensions
type1:
    dimensions:
        - date:
            question: when
            join_key: bk_date
            role_playing:
                order_date:     
                delivery_date:
type2:
    dimensions:
        - customer:
            question: who
            join_key: bk_customer
        - product:
            question: what
            join_key: bk_product
            role_playing:
                manufacturing_product:    
    dimension_as_of_date: load_datetime  
       
payload:
    - quantity_sold
    - total_sales_amount
    - discount_amount
    - tax_amount   

{%- endset -%}
{% set metadata_dict = fromyaml(yaml_metadata) %}

# Call the macro
{{ fact(
    source = metadata_dict['source'],
    name = metadata_dict['name'],
    primary_key = metadata_dict['primary_key'],
    type1_dims = metadata_dict['type1'],
    type2_dims = metadata_dict['type2'],
    payload = metadata_dict['payload'],
) }}
```

