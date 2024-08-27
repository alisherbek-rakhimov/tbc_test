drop table if exists public.customers;
create table customers
(
    id      bigserial primary key,
    name    varchar(255) not null,
    email   varchar(255) not null,
    country varchar(255) not null,
    load_dt timestamp default current_timestamp
);

drop table if exists public.products;
create table products
(
    id       bigserial primary key,
    name     varchar(255)   not null,
    price    decimal(10, 2) not null,
    category varchar(255),
    load_dt  timestamp default current_timestamp
);

drop table if exists public.sales_transactions;
create table sales_transactions
(
    id            bigserial primary key,
    customer_id   int references customers (id) on delete cascade,
    product_id    int references products (id) on delete cascade,
    purchase_date timestamp not null default current_timestamp,
    quantity      int       not null check (quantity > 0),
    load_dt       timestamp          default current_timestamp
)
    DISTRIBUTED BY (id);

drop table if exists public.shipping_details;
create table shipping_details
(
    id               bigserial primary key ,
    transaction_id   int references sales_transactions (id) on delete cascade,
    shipping_date    timestamp    not null default current_timestamp,
    shipping_address varchar(255) not null,
    city             varchar(255) not null,
    country          varchar(255) not null,
    load_dt          timestamp             default current_timestamp
) distributed by (id);


with total_sales_per_month as (select date_trunc('month', st.purchase_date) as month,
                                      count(st.id)                          as total_transactions,
                                      sum(st.quantity * p.price)            as total_sales_amount
                               from sales_transactions st
                                        join
                                    products p on st.product_id = p.id
                               group by month
                               order by month desc)

select *, avg(total_sales_amount) over (order by month desc rows between 3 preceding and current row)
from total_sales_per_month;
