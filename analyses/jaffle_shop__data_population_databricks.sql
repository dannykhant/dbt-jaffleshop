create or replace table raw.jaffle_shop.customers 
( id string,
  name string );

copy into raw.jaffle_shop.customers (id, name)
from 's3://dbt-learn-sample-data/raw_customers.csv'
fileformat = CSV
format_options ('mergeSchema' = 'true', 'skipRows'='1');

create or replace table raw.jaffle_shop.orders
( id string,
  customer string,
  ordered_at timestamp,
  store_id string,
  subtotal int,
  tax_paid int,
  order_total int
);

copy into raw.jaffle_shop.orders 
(id,customer,ordered_at,store_id,subtotal,tax_paid,order_total)
from 's3://dbt-learn-sample-data/raw_orders.csv'
fileformat = CSV
format_options ('mergeSchema' = 'true', 'skipRows'='1');

create or replace table raw.jaffle_shop.items
( id string,
  order_id string,
  sku string
);

copy into raw.jaffle_shop.items (id,order_id,sku)
from 's3://dbt-learn-sample-data/raw_items.csv'
fileformat = CSV
format_options ('mergeSchema' = 'true', 'skipRows'='1');

create or replace table raw.jaffle_shop.products 
( sku string,
  name string,
  type string,
  price int,
  description string
);

copy into raw.jaffle_shop.products (sku,name,type,price,description)
from 's3://dbt-learn-sample-data/raw_products.csv'
fileformat = CSV
format_options ('mergeSchema' = 'true', 'skipRows'='1');

create or replace table raw.jaffle_shop.stores 
( id string,
  name string,
  opened_at timestamp,
  tax_rate double
);

copy into raw.jaffle_shop.stores(id,name,opened_at,tax_rate)
from 's3://dbt-learn-sample-data/raw_stores.csv'
fileformat = CSV
format_options ('mergeSchema' = 'true', 'skipRows'='1');

create or replace table raw.jaffle_shop.supplies
( id string,
  name string,
  cost double,
  perishable boolean,
  sku string
);

copy into raw.jaffle_shop.supplies (id,name,cost,perishable,sku)
from 's3://dbt-learn-sample-data/raw_supplies.csv'
fileformat = CSV
format_options ('mergeSchema' = 'true', 'skipRows'='1');