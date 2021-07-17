create table Entries (
order_row_id uuid,
quantity int,
unit_price float,
discount float
);
Create table Orders (
row_id uuid,
customerid varchar
);
Create table Customer (
row_id uuid,
customerid varchar
);
create materialized view Entries_view as
select order_row_id, quantity * unit_price - COALESCE(discount, 0.0) as total, quantity, unit_price, discount
from Entries;
create materialized view Orders_view as
select
    o.row_id,
    o.customerid,
    e1.total,
    e2.total_savings
from Orders o
         left outer join (select order_row_id, sum(e.total) as total
                          from Entries_view e
                          group by e.order_row_id) e1 on o.row_id = e1.order_row_id
         left outer join (select order_row_id, sum(COALESCE(e.discount, 0.0)) as total_savings
                          from Entries_view e
                          group by e.order_row_id) e2 on o.row_id = e2.order_row_id;

create materialized view Customer_view as
select c.customerid, o2.total_orders
from Customer c
         left outer join (select o.customerid, sum(o.total) as total_orders
                          from Orders_view o
                          group by o.customerid) o2 on o2.customerid = c.customerid;
insert into Orders(row_id, customerid) values('90f36993-ef6b-403f-ab5b-0087384e322b', '1000101');
insert into Entries(order_row_id, quantity, unit_price, discount) values ('90f36993-ef6b-403f-ab5b-0087384e322b', 1, 17.35, null);
insert into Entries(order_row_id, quantity, unit_price, discount) values ('90f36993-ef6b-403f-ab5b-0087384e322b', 2, 57.50, 11.50);
insert into Orders(row_id, customerid) values('adb6c5f8-067a-4292-91b5-ac143467def0','1000107');
insert into Entries(order_row_id, quantity, unit_price, discount) values ('adb6c5f8-067a-4292-91b5-ac143467def0', 1, 41.95, 0);
insert into Orders(row_id, customerid) values('c2bc2e81-940a-4a1c-8bc5-6b00ad50372a','1000107');
insert into Entries(order_row_id, quantity, unit_price, discount) values ('c2bc2e81-940a-4a1c-8bc5-6b00ad50372a', 8, 8.49, 0);
insert into Entries(order_row_id, quantity, unit_price, discount) values ('c2bc2e81-940a-4a1c-8bc5-6b00ad50372a', 1, 41.95, 5.00);
insert into Customer(row_id, customerid) values ('98b617d0-6b01-48ca-aff0-73f985130ceb', '1000101');
insert into Customer(row_id, customerid) values ('cec9260e-05e0-4103-8b28-2d7d1fd7a107', '1000107');
refresh materialized view Entries_view;
refresh materialized view Orders_view;
refresh materialized view Customer_view;
select * from Entries_view;
select * from Orders_view;
select * from Customer_view;


drop materialized view Entries_view;
drop materialized view Customer_view;
drop materialized view Orders_view;
drop table Customer;
drop table Orders;
drop table Entries;

select jsonb_agg(to_jsonb(o)) orders from
  (select * from Orders_view left outer join (select * from Entries_view) ev on Orders_view.row_id = ev.order_row_id) o;