---Jinja
{%- set payment_methods = ['bank_transfer', 'credit_card', 'coupon', 'gift_card'] -%} 

with payments as 
	( select * from {{ ref('stg_payments') }} ), 
final as 
	( select order_id, 
	{% for payment_method in payment_methods -%} 
		sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount 
		{%- if not loop.last -%} 
		,
		{% endif -%} 
	{%- endfor %} 
from payments group by 1 ) 
select * from final

-- Macros
-- macros/cents_to_dollars.sql

{% macro cents_to_dollars(column_name, decimal_places=2) -%}
    round( 1.0 * {{ column_name }} / 100, {{ decimal_places }})
{%- endmacro %}

-- Refactored stg_payments.sql

select
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status,
    -- amount is stored in cents, convert it to dollars
    {{ cents_to_dollars('amount', 4) }} as amount,
    created as created_at
from {{ source('stripe','payment') }}

-- Packages
packages.yml in the root directory of your project

packages:
  - package: dbt-labs/dbt_utils
    version: 0.7.1
models/all_dates.sql

{{ config (
    materialized="table"
)}}

{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2020-01-01' as date)",
    end_date="cast('2021-01-01' as date)"
   )
}}

---- Advance Macro
-- grant_select macro
-- Create a new file in the macros directory titled 'grant_select.sql'
-- Copy the code below and adjust it based on your environment to grant select to your role or user (depending on your data platform).  You will need to reference the target variable documentation.
{% macro grant_select(...) %}

    {% set sql %}
        ...
    {% endset %}

    {{ log(..., info=True) }}
    {% do run_query(sql) %}
    {{ log(..., info=True) }}

{% endmacro %}

-- clean_stale_models macro
-- Create a new file in the macros directory titled 'clean_stale_models.sql'
-- Copy the code below and adjust it based on your environment to drop any database objects that are older than 7 days by default.

{% macro clean_stale_models(database=target.database, schema=target.schema, days=7, dry_run=True) %}

    {% set get_drop_commands_query %}
        select
         case 
            when table_type = ...

    {% endset %}

    {{ log('\nGenerating cleanup queries...\n', info=True) }}
    {% set drop_queries = run_query(get_drop_commands_query).columns[1].values() %}

    {% for drop_query in drop_queries %}
        {% if execute and not dry_run %}
            {{ log('Dropping table/view with command: ' ~ drop_query, info=True) }}
            {% do run_query(drop_query) %}    
        {% else %}
            {{ log(drop_query, info=True) }}
        {% endif %}
    {% endfor %}
  
{% endmacro %}

-- Exemplar
-- Use the following code snippets to assist you in the previous exercise and check your work. Disclaimer, for the sake of brevity, these solutions are in a Snowflake environment. 

-- grant_select macro
{% macro grant_select(schema=target.schema, role=target.role) %}

    {% set sql %}
        grant usage on schema {{ schema }} to role {{ role }};
        grant select on all tables in schema {{ schema }} to role {{ role }};
        grant select on all views in schema {{ schema }} to role {{ role }};
    {% endset %}

    {{ log('Granting select on all tables and views in schema ' ~ target.schema ~ ' to role ' ~ role, info=True) }}
    {% do run_query(sql) %}
    {{ log('Privileges granted', info=True) }}

{% endmacro %}

-- clean_stale_models macro

{% macro clean_stale_models(database=target.database, schema=target.schema, days=7, dry_run=True) %}
    
    {% set get_drop_commands_query %}
        select
            case 
                when table_type = 'VIEW'
                    then table_type
                else 
                    'TABLE'
            end as drop_type, 
            'DROP ' || drop_type || ' {{ database | upper }}.' || table_schema || '.' || table_name || ';'
        from {{ database }}.information_schema.tables 
        where table_schema = upper('{{ schema }}')
        and last_altered <= current_date - {{ days }} 
    {% endset %}

    {{ log('\nGenerating cleanup queries...\n', info=True) }}
    {% set drop_queries = run_query(get_drop_commands_query).columns[1].values() %}

    {% for query in drop_queries %}
        {% if dry_run %}
            {{ log(query, info=True) }}
        {% else %}
            {{ log('Dropping object with command: ' ~ query, info=True) }}
            {% do run_query(query) %} 
        {% endif %}       
    {% endfor %}
    
{% endmacro %} 