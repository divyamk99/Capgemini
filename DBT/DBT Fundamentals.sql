-- Staging Models
-- Create a staging/jaffle_shop directory in your models folder.
-- Create a stg_jaffle_shop__customers.sql model for raw.jaffle_shop.customers

select
    id as customer_id,
    first_name,
    last_name
from raw.jaffle_shop.customers

-- Create a stg_jaffle_shop__orders.sql model for raw.jaffle_shop.orders
select
    id as order_id,
    user_id as customer_id,
    order_date,
    status
from raw.jaffle_shop.orders
-- Mart Models
-- Create a marts/marketing directory in your models folder.
-- Create a dim_customers.sql model

with customers as (
     select * from {{ ref('stg_jaffle_shop__customers') }}
),
orders as ( 
    select * from {{ ref('stg_jaffle_shop__orders') }}
),
customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders
    from orders
    group by 1
),
final as (
  select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce (customer_orders.number_of_orders, 0) 
        as number_of_orders
    from customers
    left join customer_orders using (customer_id)
)
select * from final

-- Configure your materializations
-- In your dbt_project.yml file, configure the staging directory to be materialized as views.
models:
  jaffle_shop:
    staging:
      +materialized: view
-- In your dbt_project.yml file, configure the marts directory to be materialized as tables.
models:
  jaffle_shop:
  ...
    marts:
      +materialized: table
	  
-- Examplar
-- Exemplar
-- Self-check stg_stripe_payments, fct_orders, dim_customers
-- Use this page to check your work on these three models.

-- staging/stripe/stg_stripe__payments.sql

select
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status,
    -- amount is stored in cents, convert it to dollars
    amount / 100 as amount,
    created as created_at
from raw.stripe.payment 
-- marts/finance/fct_orders.sql

with orders as  (
    select * from {{ ref ('stg_jaffle_shop__orders' )}}
),
payments as (
    select * from {{ ref ('stg_stripe__payments') }}
),
order_payments as (
    select
        order_id,
        sum (case when status = 'success' then amount end) as amount
    from payments
    group by 1
),
 final as (
    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        coalesce (order_payments.amount, 0) as amount
    from orders
    left join order_payments using (order_id)
)
select * from final
-- marts/marketing/dim_customers.sql 

-- *Note: This is different from the original dim_customers.sql - you may refactor fct_orders in the process.

with customers as (
    select * from {{ ref ('stg_jaffle_shop__customers')}}
),
orders as (
    select * from {{ ref ('fct_orders')}}
),
customer_orders as (
    select
        customer_id,
        min (order_date) as first_order_date,
        max (order_date) as most_recent_order_date,
        count(order_id) as number_of_orders,
        sum(amount) as lifetime_value
    from orders
    group by 1
),
 final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce (customer_orders.number_of_orders, 0) as number_of_orders,
        customer_orders.lifetime_value
    from customers
    left join customer_orders using (customer_id)
)
select * from final


---------------------------------Source---------------------------
-- Configure sources
-- Configure a source for the tables raw.jaffle_shop.customers and raw.jaffle_shop.orders in a file called src_jaffle_shop.yml.
-- models/staging/jaffle_shop/src_jaffle_shop.yml

version: 2

sources:
  name: jaffle_shop
    database: raw
    schema: jaffle_shop
    tables:
      name: customers
      name: orders
-- Extra credit: Configure a source for the table raw.stripe.payment in a file called src_stripe.yml.
-- Refactor staging models
-- Refactor stg_jaffle_shop__customers.sql using the source function.
-- models/staging/jaffle_shop/stg_jaffle_shop__customers.sql

select 
    id as customer_id,
    first_name,
    last_name
from {{ source('jaffle_shop', 'customers') }}

-- Refactor stg_jaffle_shop__orders.sql using the source function.
-- models/staging/jaffle_shop/stg_jaffle_shop__orders.sql

select
    id as order_id,
    user_id as customer_id,
    order_date,
    status
from {{ source('jaffle_shop', 'orders') }}

-- Extra credit: Refactor stg_stripe__payments.sql using the source function.
-- Extra credit
-- Configure your Stripe payments data to check for source freshness.
-- Run dbt source freshness.
-- You can configure your src_stripe.yml file as below:

version: 2

sources:
  - name: stripe
    database: raw
    schema: stripe
    tables:
      - name: payment
        loaded_at_field: _batched_at
        freshness:
          warn_after: {count: 12, period: hour}
          error_after: {count: 24, period: hour}
		  
-- Exemplar
-- Self-check src_stripe and stg_payments
-- Use this page to check your work.
-- models/staging/stripe/src_stripe.yml

version: 2

sources:
  - name: stripe
    database: raw
    schema: stripe
    tables:
      - name: payment
	  
-- models/staging/stripe/stg_payments.sql

select
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status,
    -- amount is stored in cents, convert it to dollars
    amount / 100 as amount,
    created as created_at
from {{ source('stripe', 'payment') }}

-----------------------------------Testing------------------------

-- Generic Tests
-- Add tests to your jaffle_shop staging tables:
-- Create a file called stg_jaffle_shop.yml for configuring your tests.
-- Add unique and not_null tests to the keys for each of your staging tables.
-- Add an accepted_values test to your stg_jaffle_shop__orders model for status.
-- Run your tests.
-- models/staging/jaffle_shop/stg_jaffle_shop.yml

version: 2

models:
  - name: stg_jaffle_shop__customers
    columns: 
      - name: customer_id
        tests:
          - unique
          - not_null
      - name: stg_jaffle_shop__orders
        columns:
          - name: order_id
            tests:
              - unique
              - not_null
      - name: status
        tests:
          - accepted_values:
              values:
                - completed
                - shipped
                - returned
                - return_pending
                - placed
-- Singular Tests
-- Add the test tests/assert_positive_value_for_total_amount.sql to be run on your stg_payments model.
-- Run your tests.
-- tests/assert_positive_value_for_total_amount.sql
-- Refunds have a negative amount, so the total amount should always be >= 0.
-- Therefore return records where this isn't true to make the test fail.
select
  order_id,
        sum(amount)as total_amount
from {{ ref('stg_stripe__payments') }}
group by 1
having not (total_amount < 0)

-- Extra Credit
-- Add a relationships test to your stg_jaffle_shop__orders model for the customer_id in stg_jaffle_shop__customers.
-- 
-- Add tests throughout the rest of your models.
-- Write your own singular tests.

-- Exemplar
-- Add a relationships test to your stg_jaffle_shop__orders model for the customer_id in stg_jaffle_shop__customers.
-- 
-- models/staging/jaffle_shop/stg_jaffle_shop.yml

version:2

models:
  name: stg_jaffle_shop__customers
    columns:
      name: customer_id
        tests:
          - unique
          - not_null
  name: stg_jaffle_shop__orders
    columns:
      name: order_id
        tests:
         - unique
         - not_null
      - name: status
        tests:
          accepted_values:
              values:
                - completed
                - shipped
                - returned
                - return_pending
      name: customer_id
      tests:
        - relationships:
              to: ref('stg_jaffle_shop__customers')
              field: customer_id
			  
----------------------------------Documentation--------------------
-- Practice
-- Using the resources in this module, complete the following in your dbt project:
-- 
-- Write documentation
-- Add documentation to the file models/staging/jaffle_shop/stg_jaffle_shop.yml.
-- Add a description for your stg_jaffle_shop__customers model and the column customer_id.
-- Add a description for your stg_jaffle_shop__orders model and the column order_id.
-- Create a reference to a doc block
-- Create a doc block for your stg_jaffle_shop__orders model to document the status column.
-- Reference this doc block in the description of status in stg_jaffle_shop__orders.
-- models/staging/jaffle_shop/stg_jaffle_shop.yml

version: 2

models:
  - name: stg_jaffle_shop__customers
    description: Staged customer data from our jaffle shop app.
    columns: 
      - name: customer_id
        description: The primary key for customers.
        tests:
          - unique
          - not_null

  - name: stg_jaffle_shop__orders
    description: Staged order data from our jaffle shop app.
    columns: 
      - name: order_id
        description: Primary key for orders.
        tests:
          - unique
          - not_null
      - name: status
        description: "{{ doc('order_status') }}"
        tests:
          - accepted_values:
              values:
                - completed
                - shipped
                - returned
                - placed
                - return_pending
      - name: customer_id
        description: Foreign key to stg_customers.customer_id.
        tests:
          - relationships:
              to: ref('stg_jaffle_shop__customers')
              field: customer_id

-- models/staging/jaffle_shop/jaffle_shop.md

-- {% docs order_status %}
    
-- One of the following values: 
-- 
-- | status         | definition                                       |
-- |----------------|--------------------------------------------------|
-- | placed         | Order placed, not yet shipped                    |
-- | shipped        | Order has been shipped, not yet been delivered   |
-- | completed      | Order has been received by customers             |
-- | return pending | Customer indicated they want to return this item |
-- | returned       | Item has been returned                           |

-- {% enddocs %}
-- Generate and view documentation
-- Generate the documentation by running dbt docs generate.
-- View the documentation that you wrote for the stg_jaffle_shop__orders model.
-- View the Lineage Graph for your project.
-- Extra Credit
-- Add documentation to the other columns in stg_jaffle_shop__customers and stg_jaffle_shop__orders.
-- Add documentation to the stg_stripe__payments model.
-- Create a doc block for another place in your project and generate this in your documentation.			  