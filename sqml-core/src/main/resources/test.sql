Create table Orders (
  json jsonb
);

insert into orders(json) values ('{
    "__id": "90f36993-ef6b-403f-ab5b-0087384e322b",
    "id_number": 10007543,
    "customerid_number": 1000101,
    "time_number": 1617774237,
    "entries_object": [
      {
        "__id": "adb6c5f8-067a-4292-91b5-ac143467def0",
        "productid_number": 7235,
        "quantity_number": 1,
        "unit_price_number": 17.35,
        "discount_number": 0
      },
      {
        "__id": "c2bc2e81-940a-4a1c-8bc5-6b00ad50372a",
        "productid_number": 8757,
        "quantity_number": 2,
        "unit_price_number": 57.50,
        "discount_number": 11.50
      }
    ]
  }');
insert into orders(json) values ('{
    "__id": "f0f36993-ef6b-403f-ab5b-0087384e322b",
    "id_number": 10007543,
    "customerid_number": 1000101,
    "time_number": 1617774238,
    "entries_object":
      [{
        "__id": "fdb6c5f8-067a-4292-91b5-ac143467def0",
        "productid_number": 7236,
        "quantity_number": 1,
        "unit_price_number": 18.35,
        "discount_number": 0
      }]
  }');

Create table Customer (
  json jsonb
);
insert into customer(json) values ('{
    "__id": "00f36993-ef6b-403f-ab5b-0087384e322b",
    "customerid_number": 1000101
  }');

create materialized view Entries_view as
select __parent_id, __id, quantity * unit_price - COALESCE(discount, 0.0) as total, quantity, unit_price, discount
from (
  select orders.json->'__id' as __parent_id,
  (entries->'__id')::text as __id,
  (entries->'quantity_number')::integer as quantity,
  (entries->'unit_price_number')::float as unit_price,
  (entries->'discount_number')::float as discount
  from orders, jsonb_array_elements(json->'entries_object') entries
) e;

create materialized view Orders_view as
select
    o.__id,
    o.customerid,
    e1.total,
    e2.total_savings
from (select (json->'__id')::text as __id, (json->'customerid_number')::integer as customerid from orders) o
         left outer join (select __parent_id, sum(e.total) as total
                          from Entries_view e
                          group by e.__parent_id) e1 on o.__id::text = e1.__parent_id::text
         left outer join (select __parent_id, sum(COALESCE(e.discount, 0.0)) as total_savings
                          from Entries_view e
                          group by e.__parent_id) e2 on o.__id::text = e2.__parent_id::text;

create materialized view Customer_view as
select c.customerid, o2.total_orders
from (select (json->'customerid_number')::integer as customerid from customer) c
         left outer join (select o.customerid, sum(o.total) as total_orders
                          from Orders_view o
                          group by o.customerid) o2 on o2.customerid = c.customerid;

-- Refresh logic
refresh materialized view Entries_view;
refresh materialized view Orders_view;
refresh materialized view Customer_view;

-- Test queries
select * from Entries_view;
select * from Orders_view;
select * from Customer_view;

-- A jsonb query
select jsonb_agg(to_jsonb(o)) orders from
  (select * from Orders_view left outer join (select * from Entries_view) ev on Orders_view.__id::text = ev.__parent_id::text) o;

-- To reset
drop materialized view Entries_view;
drop materialized view Customer_view;
drop materialized view Orders_view;
drop table Customer;
drop table Orders;
drop table Entries;
